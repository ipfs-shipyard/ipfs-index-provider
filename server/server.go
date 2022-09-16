package server

import (
	"context"
	"net"
	"net/http"

	"github.com/filecoin-project/index-provider/engine"
	"github.com/ipfs-shipyard/ipfs-index-provider/indexprovider"
	"github.com/ipfs/go-datastore"
	drserver "github.com/ipfs/go-delegated-routing/server"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
)

var log = logging.Logger("server")

type Server struct {
	server *http.Server
	l      net.Listener
	e      indexprovider.EngineProxy
}

func NewServer(ds datastore.Datastore, o ...Option) (*Server, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	l, err := net.Listen("tcp", opts.listenAddr)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New()
	if err != nil {
		return nil, err
	}

	log.Info("libp2p host initialized. host_id=", h.ID())

	eng, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.HttpPublisher), engine.WithDirectAnnounce(opts.directAnnounceUrls...))
	if err != nil {
		return nil, err
	}

	ip, err := indexprovider.NewIndexProvider(context.Background(), eng, opts.ttl, opts.cidsPerChunk, ds)
	if err != nil {
		return nil, err
	}

	handler := drserver.DelegatedRoutingAsyncHandler(ip)

	server := &http.Server{
		Handler:      handler,
		ReadTimeout:  opts.readTimeout,
		WriteTimeout: opts.writeTimeout,
	}
	s := &Server{server, l, eng}
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) Start(ctx context.Context) error {
	log.Infow("admin http server listening", "addr", s.l.Addr())
	err := s.e.Start(ctx)
	if err != nil {
		return err
	}
	return s.server.Serve(s.l)
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Info("admin http server shutdown")
	err := s.e.Shutdown()
	if err != nil {
		return err
	}
	return s.server.Shutdown(ctx)
}
