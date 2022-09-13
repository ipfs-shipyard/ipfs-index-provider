package config

import (
	"time"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

const (
	defaultAdminServerAddr = "/ip4/127.0.0.1/tcp/3102"
	defaultReadTimeout     = time.Duration(30 * time.Second)
	defaultWriteTimeout    = time.Duration(30 * time.Second)
)

type ReframeServer struct {
	// Admin is the admin API listen address
	ListenMultiaddr string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

// NewReframeServer instantiates a new AdminServer config with default values.
func NewReframeServer() ReframeServer {
	return ReframeServer{
		ListenMultiaddr: defaultAdminServerAddr,
		ReadTimeout:     defaultReadTimeout,
		WriteTimeout:    defaultWriteTimeout,
	}
}

func (as *ReframeServer) ListenNetAddr() (string, error) {
	maddr, err := multiaddr.NewMultiaddr(as.ListenMultiaddr)
	if err != nil {
		return "", err
	}

	netAddr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return "", err
	}
	return netAddr.String(), nil
}

// PopulateDefaults replaces zero-values in the config with default values.
func (c *ReframeServer) PopulateDefaults() {
	if c.ListenMultiaddr == "" {
		c.ListenMultiaddr = defaultAdminServerAddr
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
}
