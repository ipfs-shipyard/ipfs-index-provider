package server_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	mock_engine "github.com/filecoin-project/index-provider/mock"
	"github.com/golang/mock/gomock"
	"github.com/ipfs-shipyard/ipfs-index-provider/listener"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"
	"github.com/ipfs/go-delegated-routing/gen/proto"
	"github.com/ipfs/go-delegated-routing/server"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	multiaddr "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var defaultMetadata metadata.Metadata = metadata.New(metadata.Bitswap{})

func TestProvideRoundtrip(t *testing.T) {
	h, err := libp2p.New()
	if err != nil {
		panic(err)
	}

	e, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	if err != nil {
		panic(err)
	}

	ttl := 24 * time.Hour
	ip, err := listener.NewIndexProvider(e, ttl, 10, 0)
	if err != nil {
		t.Fatal(err)
	}

	c1, s1 := createClientAndServer(t, ip, nil, nil)
	defer s1.Close()

	testCid := newCid("test")

	if _, err = c1.Provide(context.Background(), testCid, time.Hour); err == nil {
		t.Fatal("should get sync error on unsigned provide request.")
	}

	priv, pID := generateKeyAndIdentity(t)
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

	if rc != 24*time.Hour {
		t.Fatal("should have gotten back the the fixed server ttl")
	}
}

func TestShouldGenerateTwoChunksOfSize1(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]cid.Cid{testCid1})), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]cid.Cid{testCid2})), gomock.Eq(defaultMetadata))

	ttl := 24 * time.Hour
	ip, err := listener.NewIndexProvider(mockEng, ttl, 1, 0)
	if err != nil {
		t.Fatal(err)
	}

	c, s := createClientAndServer(t, ip, &client.Provider{
		Peer: peer.AddrInfo{
			ID:    pID,
			Addrs: []multiaddr.Multiaddr{},
		},
		ProviderProto: []client.TransferProtocol{},
	}, priv)
	defer s.Close()

	_, err = c.Provide(ctx, testCid1, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Provide(ctx, testCid2, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
}

func TestShouldGenerateOnlyChunkOfSize2(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]cid.Cid{testCid1, testCid2})), gomock.Eq(defaultMetadata))

	ttl := 24 * time.Hour
	ip, err := listener.NewIndexProvider(mockEng, ttl, 2, 0)
	if err != nil {
		t.Fatal(err)
	}

	c, s := createClientAndServer(t, ip, &client.Provider{
		Peer: peer.AddrInfo{
			ID:    pID,
			Addrs: []multiaddr.Multiaddr{},
		},
		ProviderProto: []client.TransferProtocol{},
	}, priv)
	defer s.Close()

	_, err = c.Provide(ctx, testCid1, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Provide(ctx, testCid2, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Provide(ctx, testCid3, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}

}

func generateContextID(cids []cid.Cid) []byte {
	hasher := sha256.New()
	for _, c := range cids {
		hasher.Write(c.Bytes())
	}
	return hasher.Sum(nil)
}

func newCid(s string) cid.Cid {
	testMH1, _ := multihash.Encode([]byte(s), multihash.IDENTITY)
	return cid.NewCidV1(cid.Raw, testMH1)
}

func generateKeyAndIdentity(t *testing.T) (crypto.PrivKey, peer.ID) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return priv, pID
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
