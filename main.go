package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ipfs-shipyard/ipfs-index-provider/server"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
)

var log = logging.Logger("main")

func main() {
	setLoggingConfig()
	s, err := createServer()
	if err != nil {
		panic(err)
	}

	log.Info("Starting server")
	err = s.Start(context.Background())
	if err != nil {
		panic(err)
	}
	defer func() {
		log.Info("Shutting down")
		s.Shutdown(context.Background())
	}()
}

func createServer() (*server.Server, error) {
	// --- config

	// cfg, err := config.Load("")
	// if err != nil {
	// 	if err == config.ErrNotInitialized {
	// 		return nil, errors.New("reference provider is not initialized\nTo initialize, run using the \"init\" command")
	// 	}
	// 	return nil, fmt.Errorf("cannot load config file: %w", err)
	// }

	// --- libp2p

	// _, privKey, err := cfg.Identity.Decode()
	// if err != nil {
	// 	return nil, err
	// }

	// p2pmaddr, err := multiaddr.NewMultiaddr(cfg.ReframeServer.ListenMultiaddr)
	// if err != nil {
	// 	return nil, fmt.Errorf("bad p2p address in config %s: %s", cfg.ReframeServer.ListenMultiaddr, err)
	// }

	// h, err := libp2p.New(
	// 	// Use the keypair generated during init
	// 	libp2p.Identity(privKey),
	// 	// Listen to p2p addr specified in config
	// 	libp2p.ListenAddrs(p2pmaddr),
	// )

	// Create a new libp2p host

	// Starting provider core

	// ---   datastore

	// dataStorePath, err := config.Path("", "datastore")
	// if err != nil {
	// 	return nil, err
	// }
	// err = checkWritable(dataStorePath)
	// if err != nil {
	// 	return nil, err
	// }
	// ds, err := leveldb.NewDatastore(dataStorePath, nil)
	// if err != nil {
	// 	return nil, err
	// }

	/// --- server

	s, err := server.NewServer(datastore.NewMapDatastore(), server.WithCidsPerChunk(1), server.WithDirectAnnounceUrls([]string{"http://127.0.0.1:3001"}))
	if err != nil {
		log.Info("Error initialising server %s\n", err)
		return nil, err
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

func setLoggingConfig() error {
	// Set overall log level.
	err := logging.SetLogLevel("*", "info")
	if err != nil {
		return err
	}

	// Set level for individual loggers.
	// for loggerName, level := range cfgLogging.Loggers {
	// 	err = logging.SetLogLevel(loggerName, level)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	return nil
}
