package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

type Server struct {
	address      string
	pprofEnabled bool
	logger       *zerolog.Logger
	infoProvider InfoProvider
}

type InfoProvider func(context.Context) ([]byte, error)

func NewServer(address string, pprofEnabled bool, logger *zerolog.Logger) *Server {
	return &Server{
		address:      address,
		pprofEnabled: pprofEnabled,
		logger:       logger,
	}
}

func (s *Server) SetInfoProvider(provider InfoProvider) {
	if s == nil {
		return
	}

	s.infoProvider = provider
}

func (s *Server) Start(ctx context.Context) error {
	if s == nil || s.address == "" {
		return nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if s.infoProvider != nil {
		mux.HandleFunc("/v1/info", s.handleInfo)
	}
	if s.pprofEnabled {
		registerPprofHandlers(mux)
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	server := &http.Server{
		Addr:              s.address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if s.logger != nil {
			s.logger.Info().Str("address", s.address).Msg("starting observability server")
		}

		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown observability server: %w", err)
		}

		err := <-errCh
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("observability server stopped: %w", err)
		}

		return nil
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("observability server failed: %w", err)
		}

		return nil
	}
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}

	data, err := s.infoProvider(r.Context())
	if err != nil {
		if s.logger != nil {
			s.logger.Error().Err(err).Msg("build observability info")
		}
		http.Error(w, "failed to build info", http.StatusInternalServerError)

		return
	}
	if !json.Valid(data) {
		if s.logger != nil {
			s.logger.Error().Msg("observability info provider returned invalid json")
		}
		http.Error(w, "invalid info", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(append(data, '\n'))
}

func registerPprofHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
