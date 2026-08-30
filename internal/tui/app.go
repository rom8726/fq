package tui

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/dbcli"
	"github.com/fq-db/fq/internal/network"
)

const (
	connectMaxWait       = 5 * time.Second
	connectRetryInterval = 100 * time.Millisecond
	inputPrompt          = "[fq]> "
	maxPaneLines         = 2000

	boxHorizontal  = '─'
	boxVertical    = '│'
	boxTopLeft     = '┌'
	boxTopRight    = '┐'
	boxBottomLeft  = '└'
	boxBottomRight = '┘'
)

type Config struct {
	Address        string
	MaxMessageSize int
	IdleTimeout    time.Duration
	Logger         *zerolog.Logger
}

type pane int

const (
	logPane pane = iota
	outputPane
)

var (
	styleBorderLog     = tcell.StyleDefault.Foreground(tcell.ColorSteelBlue)
	styleBorderOutput  = tcell.StyleDefault.Foreground(tcell.ColorDarkCyan)
	styleBorderInput   = tcell.StyleDefault.Foreground(tcell.ColorDarkSlateGray)
	styleInputPrompt   = tcell.StyleDefault.Foreground(tcell.ColorGreen)
	styleInputText     = tcell.StyleDefault.Foreground(tcell.ColorWhite)
	styleLogDebug      = tcell.StyleDefault.Foreground(tcell.ColorGray)
	styleLogInfo       = tcell.StyleDefault.Foreground(tcell.ColorGreen)
	styleLogWarn       = tcell.StyleDefault.Foreground(tcell.ColorYellow)
	styleLogError      = tcell.StyleDefault.Foreground(tcell.ColorRed)
	styleOutputSuccess = tcell.StyleDefault.Foreground(tcell.ColorGreen)
	styleOutputWarn    = tcell.StyleDefault.Foreground(tcell.ColorYellow)
	styleOutputError   = tcell.StyleDefault.Foreground(tcell.ColorRed)
)

type styledLine struct {
	text  string
	style tcell.Style
}

type App struct {
	mu          sync.Mutex
	screen      tcell.Screen
	logLines    []styledLine
	outputLines []styledLine

	clientMu  sync.Mutex
	clientUse sync.Mutex
	client    *network.TCPClient

	input      []rune
	cursor     int
	history    []string
	historyPos int
}

func New() *App {
	return &App{
		logLines:    []styledLine{{style: styleLogInfo}},
		outputLines: []styledLine{{style: styleOutputSuccess}},
	}
}

func (a *App) LogWriter() io.Writer {
	return paneWriter{app: a, pane: logPane}
}

func (a *App) outputWriter() io.Writer {
	return paneWriter{app: a, pane: outputPane}
}

func (a *App) Run(ctx context.Context, cancel context.CancelFunc, cfg Config) error {
	screen, err := tcell.NewScreen()
	if err != nil {
		cancel()

		return err
	}
	if err := screen.Init(); err != nil {
		cancel()

		return err
	}
	defer screen.Fini()

	a.mu.Lock()
	a.screen = screen
	a.mu.Unlock()

	go func() {
		<-ctx.Done()
		_ = screen.PostEvent(tcell.NewEventInterrupt(nil))
	}()

	go a.connectAndServe(ctx, cancel, cfg)

	a.draw(screen)
	for {
		event := screen.PollEvent()
		if ctx.Err() != nil {
			cancel()

			return nil
		}

		switch ev := event.(type) {
		case *tcell.EventResize:
			screen.Sync()
		case *tcell.EventKey:
			a.handleKey(ctx, cancel, cfg, ev)
		}

		a.draw(screen)
	}
}

func (a *App) connectAndServe(ctx context.Context, cancel context.CancelFunc, cfg Config) {
	client, err := dialWithRetry(
		ctx,
		dialAddress(cfg.Address),
		cfg.MaxMessageSize,
		cfg.IdleTimeout,
		connectMaxWait,
		connectRetryInterval,
	)
	if err != nil {
		_, _ = fmt.Fprintf(a.outputWriter(), "failed to connect to %s: %s\n", cfg.Address, err)
		cancel()

		return
	}

	a.clientMu.Lock()
	a.client = client
	a.clientMu.Unlock()
	defer func() {
		a.clientMu.Lock()
		if a.client == client {
			a.client = nil
		}
		a.clientMu.Unlock()
		_ = client.Close()
	}()

	<-ctx.Done()
}

func (a *App) handleKey(ctx context.Context, cancel context.CancelFunc, cfg Config, event *tcell.EventKey) {
	switch event.Key() {
	case tcell.KeyCtrlC:
		cancel()
	case tcell.KeyEnter:
		a.submit(ctx, cancel, cfg)
	case tcell.KeyBackspace, tcell.KeyBackspace2:
		if a.cursor > 0 {
			a.input = append(a.input[:a.cursor-1], a.input[a.cursor:]...)
			a.cursor--
		}
	case tcell.KeyDelete:
		if a.cursor < len(a.input) {
			a.input = append(a.input[:a.cursor], a.input[a.cursor+1:]...)
		}
	case tcell.KeyLeft:
		if a.cursor > 0 {
			a.cursor--
		}
	case tcell.KeyRight:
		if a.cursor < len(a.input) {
			a.cursor++
		}
	case tcell.KeyHome:
		a.cursor = 0
	case tcell.KeyEnd:
		a.cursor = len(a.input)
	case tcell.KeyUp:
		a.historyUp()
	case tcell.KeyDown:
		a.historyDown()
	case tcell.KeyRune:
		a.input = append(a.input[:a.cursor], append([]rune{event.Rune()}, a.input[a.cursor:]...)...)
		a.cursor++
	}
}

func (a *App) submit(ctx context.Context, cancel context.CancelFunc, cfg Config) {
	request := string(a.input)
	a.input = a.input[:0]
	a.cursor = 0

	if request == "" {
		return
	}

	if dbcli.IsQuitCommand(request) {
		cancel()

		return
	}

	a.clientMu.Lock()
	client := a.client
	a.clientMu.Unlock()
	if client == nil {
		_, _ = fmt.Fprintln(a.outputWriter(), "not connected yet")

		return
	}

	a.history = append(a.history, request)
	a.historyPos = len(a.history)

	start := time.Now()
	go func() {
		a.clientUse.Lock()
		defer a.clientUse.Unlock()

		if err := dbcli.Execute(ctx, cfg.Logger, client, request, a.outputWriter(), start); err != nil {
			_, _ = fmt.Fprintf(a.outputWriter(), "connection lost: %s\n", err)
			cancel()
		}
	}()
}

func (a *App) historyUp() {
	if a.historyPos > 0 {
		a.historyPos--
		a.setInput(a.history[a.historyPos])
	}
}

func (a *App) historyDown() {
	switch {
	case a.historyPos < len(a.history)-1:
		a.historyPos++
		a.setInput(a.history[a.historyPos])
	default:
		a.historyPos = len(a.history)
		a.setInput("")
	}
}

func (a *App) setInput(text string) {
	a.input = []rune(text)
	a.cursor = len(a.input)
}

func (a *App) appendText(p pane, text string) {
	style := styleForText(p, text)
	text = stripANSI(text)

	a.mu.Lock()
	switch p {
	case logPane:
		a.logLines = appendLines(a.logLines, text, style)
	case outputPane:
		a.outputLines = appendLines(a.outputLines, text, style)
	}
	screen := a.screen
	a.mu.Unlock()

	if screen != nil {
		_ = screen.PostEvent(tcell.NewEventInterrupt(nil))
	}
}

func appendLines(lines []styledLine, text string, style tcell.Style) []styledLine {
	if len(lines) == 0 {
		lines = []styledLine{{style: style}}
	}

	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")

	parts := strings.Split(text, "\n")
	lines[len(lines)-1].text += parts[0]
	lines[len(lines)-1].style = style
	for _, part := range parts[1:] {
		lines = append(lines, styledLine{text: part, style: style})
	}

	if len(lines) > maxPaneLines {
		lines = lines[len(lines)-maxPaneLines:]
	}

	return lines
}

func styleForText(p pane, text string) tcell.Style {
	switch p {
	case logPane:
		return styleForLog(text)
	case outputPane:
		return styleForOutput(text)
	default:
		return tcell.StyleDefault
	}
}

func styleForLog(text string) tcell.Style {
	lower := strings.ToLower(stripANSI(text))
	switch {
	case strings.Contains(text, "\x1b[31m") || strings.Contains(lower, " err ") ||
		strings.Contains(lower, " error ") || strings.Contains(lower, "level=error"):
		return styleLogError
	case strings.Contains(text, "\x1b[33m") || strings.Contains(lower, " wrn ") ||
		strings.Contains(lower, " warn ") || strings.Contains(lower, "level=warn"):
		return styleLogWarn
	case strings.Contains(lower, " dbg ") || strings.Contains(lower, " debug ") ||
		strings.Contains(lower, "level=debug"):
		return styleLogDebug
	default:
		return styleLogInfo
	}
}

func styleForOutput(text string) tcell.Style {
	lower := strings.ToLower(stripANSI(text))
	switch {
	case strings.Contains(text, "\x1b[31m") || strings.Contains(lower, "error") ||
		strings.Contains(lower, "failed") || strings.Contains(lower, "malformed") ||
		strings.Contains(lower, "connection lost") || strings.Contains(lower, "not connected"):
		return styleOutputError
	case strings.Contains(text, "\x1b[33m") || strings.Contains(lower, "warning") ||
		strings.Contains(lower, "timeout"):
		return styleOutputWarn
	default:
		return styleOutputSuccess
	}
}

type paneWriter struct {
	app  *App
	pane pane
}

func (w paneWriter) Write(p []byte) (int, error) {
	w.app.appendText(w.pane, string(p))

	return len(p), nil
}

func (a *App) draw(screen tcell.Screen) {
	width, height := screen.Size()
	if width <= 0 || height <= 0 {
		return
	}

	a.mu.Lock()
	logLines := append([]styledLine(nil), a.logLines...)
	outputLines := append([]styledLine(nil), a.outputLines...)
	input := append([]rune(nil), a.input...)
	cursor := a.cursor
	a.mu.Unlock()

	screen.Clear()

	inputHeight := 3
	if height < inputHeight {
		inputHeight = height
	}
	contentHeight := height - inputHeight
	logHeight := contentHeight / 2
	outputHeight := contentHeight - logHeight
	if logHeight > 0 {
		drawBox(screen, 0, 0, width, logHeight, " log ", styleBorderLog)
		drawLines(screen, 1, 1, width-2, logHeight-2, logLines)
	}
	if outputHeight > 0 {
		y := logHeight
		drawBox(screen, 0, y, width, outputHeight, " output ", styleBorderOutput)
		drawLines(screen, 1, y+1, width-2, outputHeight-2, outputLines)
	}
	if inputHeight > 0 {
		y := height - inputHeight
		drawBox(screen, 0, y, width, inputHeight, " command ", styleBorderInput)
		drawInput(screen, 1, y+1, width-2, input, cursor)
	}

	screen.Show()
}

func drawBox(screen tcell.Screen, x, y, width, height int, title string, style tcell.Style) {
	if width <= 0 || height <= 0 {
		return
	}

	for col := x; col < x+width; col++ {
		screen.SetContent(col, y, boxHorizontal, nil, style)
		screen.SetContent(col, y+height-1, boxHorizontal, nil, style)
	}
	for row := y; row < y+height; row++ {
		screen.SetContent(x, row, boxVertical, nil, style)
		screen.SetContent(x+width-1, row, boxVertical, nil, style)
	}
	screen.SetContent(x, y, boxTopLeft, nil, style)
	screen.SetContent(x+width-1, y, boxTopRight, nil, style)
	screen.SetContent(x, y+height-1, boxBottomLeft, nil, style)
	screen.SetContent(x+width-1, y+height-1, boxBottomRight, nil, style)

	drawText(screen, x+2, y, width-4, []rune(title), style)
}

func drawLines(screen tcell.Screen, x, y, width, height int, lines []styledLine) {
	if width <= 0 || height <= 0 || len(lines) == 0 {
		return
	}

	start := 0
	if len(lines) > height {
		start = len(lines) - height
	}
	for row, line := range lines[start:] {
		drawText(screen, x, y+row, width, []rune(line.text), line.style)
	}
}

func drawInput(screen tcell.Screen, x, y, width int, input []rune, cursor int) {
	if width <= 0 {
		return
	}

	prompt := []rune(inputPrompt)
	available := width - len(prompt)
	if available < 0 {
		available = 0
	}

	offset := 0
	if cursor > available {
		offset = cursor - available
	}
	visibleInput := input //nolint:ineffassign // ok
	if offset < len(input) {
		visibleInput = input[offset:]
	} else {
		visibleInput = nil
	}

	drawText(screen, x, y, len(prompt), prompt, styleInputPrompt)
	drawText(screen, x+len(prompt), y, width-len(prompt), visibleInput, styleInputText)
	cursorX := x + len(prompt) + cursor - offset
	if cursorX >= x+width {
		cursorX = x + width - 1
	}
	screen.ShowCursor(cursorX, y)
}

func drawText(screen tcell.Screen, x, y, width int, text []rune, style tcell.Style) {
	if width <= 0 {
		return
	}
	if len(text) > width {
		text = text[:width]
	}
	for i, ch := range text {
		screen.SetContent(x+i, y, ch, nil, style)
	}
}

func stripANSI(text string) string {
	var out strings.Builder
	out.Grow(len(text))

	for i := 0; i < len(text); i++ {
		if text[i] != '\x1b' || i+1 >= len(text) || text[i+1] != '[' {
			out.WriteByte(text[i])
			continue
		}

		i += 2
		for i < len(text) {
			ch := text[i]
			if ch >= '@' && ch <= '~' {
				break
			}
			i++
		}
	}

	return out.String()
}
