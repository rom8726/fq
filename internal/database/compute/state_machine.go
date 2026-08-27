package compute

const (
	foundLetterEvent = iota
	foundWhiteSpaceEvent
	// must be last
	eventsNumber
)

const (
	initialState = iota
	wordState
	whiteSpaceState
	invalidState
	// must be last
	statesNumber
)

type transition struct {
	jump   jumpKind
	action actionKind
}

type jumpKind int

const (
	noJump jumpKind = iota
	appendLetterJump
	skipWhiteSpaceJump
)

type actionKind int

const (
	noAction actionKind = iota
	addTokenAction
)

type compiledStateMachine struct {
	transitions [statesNumber][eventsNumber]transition
}

type stateMachineRun struct {
	machine    *compiledStateMachine
	state      int
	position   int
	tokenStart int
	tokens     []string
	query      string
}

func newStateMachine() *compiledStateMachine {
	return &compiledStateMachine{
		transitions: [statesNumber][eventsNumber]transition{
			initialState: {
				foundLetterEvent:     transition{jump: appendLetterJump},
				foundWhiteSpaceEvent: transition{jump: skipWhiteSpaceJump},
			},
			wordState: {
				foundLetterEvent:     transition{jump: appendLetterJump},
				foundWhiteSpaceEvent: transition{jump: skipWhiteSpaceJump, action: addTokenAction},
			},
			whiteSpaceState: {
				foundLetterEvent:     transition{jump: appendLetterJump},
				foundWhiteSpaceEvent: transition{jump: skipWhiteSpaceJump},
			},
			invalidState: {},
		},
	}
}

func (sm *compiledStateMachine) parse(query string) ([]string, error) {
	run := stateMachineRun{
		machine:    sm,
		state:      initialState,
		tokenStart: -1,
		query:      query,
	}

	return run.parse(query)
}

func (run *stateMachineRun) parse(query string) ([]string, error) {
	for i := 0; i < len(query); i++ {
		symbol := query[i]
		run.position = i
		switch {
		case isWhiteSpace(symbol):
			run.processEvent(foundWhiteSpaceEvent)
		case isLetter(symbol):
			run.processEvent(foundLetterEvent)
		default:
			return nil, ErrInvalidSymbol
		}
	}

	run.position = len(query)
	run.processEvent(foundWhiteSpaceEvent)

	return run.tokens, nil
}

func (run *stateMachineRun) processEvent(event int) {
	ts := run.machine.transitions[run.state][event]
	run.state = run.jump(ts.jump)
	run.action(ts.action)
}

func (run *stateMachineRun) jump(jump jumpKind) int {
	switch jump {
	case appendLetterJump:
		if run.tokenStart < 0 {
			run.tokenStart = run.position
		}

		return wordState
	case skipWhiteSpaceJump:
		return whiteSpaceState
	default:
		return invalidState
	}
}

func (run *stateMachineRun) action(action actionKind) {
	if action == addTokenAction {
		run.tokens = append(run.tokens, run.query[run.tokenStart:run.position])
		run.tokenStart = -1
	}
}
