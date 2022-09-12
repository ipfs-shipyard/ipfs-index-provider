package server_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	datatransfer "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/engine/policy"
	"github.com/ipfs-shipyard/ipfs-index-provider/indexprovider/internal/config"
	"github.com/ipfs-shipyard/ipfs-index-provider/server"
	leveldb "github.com/ipfs/go-ds-leveldb"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	logging "github.com/ipfs/go-log/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	"github.com/mitchellh/go-homedir"
	"github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("server-test")

func TestServer(t *testing.T) {
	createServer()
}

func createServer() (*server.Server, error) {
	// --- config

	cfg, err := config.Load("")
	if err != nil {
		if err == config.ErrNotInitialized {
			return nil, errors.New("reference provider is not initialized\nTo initialize, run using the \"init\" command")
		}
		return nil, fmt.Errorf("cannot load config file: %w", err)
	}

	// --- libp2p

	_, privKey, err := cfg.Identity.Decode()
	if err != nil {
		return err
	}

	p2pmaddr, err := multiaddr.NewMultiaddr(cfg.ProviderServer.ListenMultiaddr)
	if err != nil {
		return nil, fmt.Errorf("bad p2p address in config %s: %s", cfg.ProviderServer.ListenMultiaddr, err)
	}

	h, err := libp2p.New(
		// Use the keypair generated during init
		libp2p.Identity(privKey),
		// Listen to p2p addr specified in config
		libp2p.ListenAddrs(p2pmaddr),
	)
	if err != nil {
		return err
	}

	log.Infow("libp2p host initialized", "host_id", h.ID(), "multiaddr", p2pmaddr)

	// ---   datastore

	if cfg.Datastore.Type != "levelds" {
		return nil, fmt.Errorf("only levelds datastore type supported, %q not supported", cfg.Datastore.Type)
	}
	dataStorePath, err := config.Path("", cfg.Datastore.Dir)
	if err != nil {
		return err
	}
	err = checkWritable(dataStorePath)
	if err != nil {
		return err
	}
	ds, err := leveldb.NewDatastore(dataStorePath, nil)
	if err != nil {
		return err
	}

	// --- datatransfer

	gsnet := gsnet.NewFromLibp2pHost(h)
	dtNet := dtnetwork.NewFromLibp2pHost(h)
	gs := gsimpl.New(context.Background(), gsnet, cidlink.DefaultLinkSystem())
	tp := gstransport.NewTransport(h.ID(), gs)

	dt, err := datatransfer.NewDataTransfer(ds, dtNet, tp)
	if err != nil {
		return err
	}
	err = dt.Start(context.Background())
	if err != nil {
		return err
	}

	// --- sync policy

	syncPolicy, err := policy.New(cfg.Ingest.SyncPolicy.Allow, cfg.Ingest.SyncPolicy.Except)
	if err != nil {
		return err
	}

	// Starting provider core
	eng, err := engine.New(
		engine.WithDatastore(ds),
		engine.WithDataTransfer(dt),
		// engine.WithDirectAnnounce(cfg.DirectAnnounce.URLs...),
		engine.WithHost(h),
		engine.WithEntriesCacheCapacity(cfg.Ingest.LinkCacheSize),
		engine.WithChainedEntries(cfg.Ingest.LinkedChunkSize),
		engine.WithTopicName(cfg.Ingest.PubSubTopic),
		engine.WithPublisherKind(engine.PublisherKind(cfg.Ingest.PublisherKind)),
		engine.WithSyncPolicy(syncPolicy))
	if err != nil {
		return err
	}

	s, err := server.New(eng, server.WithCidsPerChunk(10))
	if err != nil {
		log.Error("Error initialising server", err)
		return err
	}

	return s, nil
}

// checkWritable checks the the directory is writable.
// If the directory does not exist it is created with writable permission.
func checkWritable(dir string) error {
	if dir == "" {
		return errors.New("cannot check empty directory")
	}

	var err error
	dir, err = homedir.Expand(dir)
	if err != nil {
		return err
	}

	if _, err = os.Stat(dir); err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			// dir doesn't exist, check that we can create it
			return os.Mkdir(dir, 0o775)
		case errors.Is(err, os.ErrPermission):
			return fmt.Errorf("cannot write to %s, incorrect permissions", err)
		default:
			return err
		}
	}

	// dir exists, make sure we can write to it
	testfile := filepath.Join(dir, "test")
	fi, err := os.Create(testfile)
	if err != nil {
		if os.IsPermission(err) {
			return fmt.Errorf("%s is not writeable by the current user", dir)
		}
		return fmt.Errorf("unexpected error while checking writeablility of repo root: %s", err)
	}
	fi.Close()
	return os.Remove(testfile)
}
