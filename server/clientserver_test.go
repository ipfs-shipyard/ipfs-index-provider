package server_test

import (
	"context"
	"crypto/rand"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs-shipyard/ipfs-index-provider/listener"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"
	"github.com/ipfs/go-delegated-routing/gen/proto"
	"github.com/ipfs/go-delegated-routing/server"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

func TestProvideRoundtrip(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	ip, err := listener.NewIndexProvider(nil)
	if err != nil {
		t.Fatal(err)
	}

	c1, s1 := createClientAndServer(t, ip, nil, nil)
	defer s1.Close()

	testMH, _ := multihash.Encode([]byte("test"), multihash.IDENTITY)
	testCid := cid.NewCidV1(cid.Raw, testMH)

	if _, err = c1.Provide(context.Background(), testCid, time.Hour); err == nil {
		t.Fatal("should get sync error on unsigned provide request.")
	}

	c, s := createClientAndServer(t, ip, &client.Provider{
		Peer: peer.AddrInfo{
			ID:    pID,
			Addrs: []multiaddr.Multiaddr{},
		},
		ProviderProto: []client.TransferProtocol{},
	}, priv)
	defer s.Close()

	rc, err := c.Provide(context.Background(), testCid, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	if rc != time.Hour {
		t.Fatal("should have gotten back the the fixed server ttl")
	}
}

func createClientAndServer(t *testing.T, service server.DelegatedRoutingService, p *client.Provider, identity crypto.PrivKey) (*client.Client, *httptest.Server) {
	// start a server
	s := httptest.NewServer(server.DelegatedRoutingAsyncHandler(service))

	// start a client
	q, err := proto.New_DelegatedRouting_Client(s.URL, proto.DelegatedRouting_Client_WithHTTPClient(s.Client()))
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.NewClient(q, p, identity)
	if err != nil {
		t.Fatal(err)
	}

	return c, s
}
