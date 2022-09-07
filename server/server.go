package server

import (
	"context"
	"net"
	"net/http"

	"github.com/filecoin-project/index-provider/engine"
	"github.com/ipfs-shipyard/ipfs-index-provider/listener"
	drserver "github.com/ipfs/go-delegated-routing/server"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
)

var log = logging.Logger("server")

type Server struct {
	server *http.Server
	l      net.Listener
	h      host.Host
	e      *engine.Engine
}

func New(h host.Host, e *engine.Engine, o ...Option) (*Server, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", opts.listenAddr)
	if err != nil {
		return nil, err
	}

	ip, err := listener.NewIndexProvider(e, opts.ttl, opts.cidsPerChunk, 0)
	if err != nil {
		return nil, err
	}

	handler := drserver.DelegatedRoutingAsyncHandler(ip)

	server := &http.Server{
		Handler:      handler,
		ReadTimeout:  opts.readTimeout,
		WriteTimeout: opts.writeTimeout,
	}
	s := &Server{server, l, h, e}
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) Start() error {
	log.Infow("admin http server listening", "addr", s.l.Addr())
	return s.server.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Info("admin http server shutdown")
	return s.server.Shutdown(ctx)
}
