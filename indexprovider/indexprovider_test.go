package indexprovider_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	mock_engine "github.com/filecoin-project/index-provider/mock"
	"github.com/golang/mock/gomock"
	ipfsip "github.com/ipfs-shipyard/ipfs-index-provider/indexprovider"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
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

func testNonceGen() []byte {
	return []byte{1, 2, 3, 4, 5}
}

func TestProvideRoundtrip(t *testing.T) {

	/**
	  err := checkWritable(datastoreDir)
	  	if err != nil {
	  		return nil, err
	  	}
	  	ds, err := leveldb.NewDatastore(datastoreDir, nil)
	  	if err != nil {
	  		return nil, err
	  	}

	*/

	h, err := libp2p.New()
	if err != nil {
		panic(err)
	}

	e, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	if err != nil {
		panic(err)
	}

	ttl := 24 * time.Hour
	ip, err := ipfsip.NewIndexProvider(context.Background(), e, ttl, 10, datastore.NewMapDatastore())
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

	rc := provide(t, c, context.Background(), testCid)

	if rc != 24*time.Hour {
		t.Fatal("should have gotten back the the fixed server ttl")
	}
}

func TestShouldAdvertiseTwoChunksWithOneCidInEach(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	ttl := 24 * time.Hour
	ip, err := ipfsip.NewIndexProviderWithNonceGen(context.Background(), mockEng, ttl, 1, datastore.NewMapDatastore(), testNonceGen)
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

	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid2)
}

func TestShouldAdvertiseOneChunkWithTwoCidsInIt(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	ttl := 24 * time.Hour
	ip, err := ipfsip.NewIndexProviderWithNonceGen(context.Background(), mockEng, ttl, 2, datastore.NewMapDatastore(), testNonceGen)
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

	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid2)
	provide(t, c, ctx, testCid3)
}

func TestShouldNotReAdvertiseRepeatedCids(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid := newCid("test")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	ttl := 24 * time.Hour
	ip, err := ipfsip.NewIndexProviderWithNonceGen(context.Background(), mockEng, ttl, 1, datastore.NewMapDatastore(), testNonceGen)
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

	provide(t, c, ctx, testCid)
	provide(t, c, ctx, testCid)
}

func TestExpiredCidsShouldBeReadvertisedIfProvidedAgain(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	ttl := time.Second
	ip, err := ipfsip.NewIndexProviderWithNonceGen(context.Background(), mockEng, ttl, 1, datastore.NewMapDatastore(), testNonceGen)
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

	provide(t, c, ctx, testCid1)
	time.Sleep(time.Second)
	provide(t, c, ctx, testCid2)
	provide(t, c, ctx, testCid1)
}

func TestShouldRemoveExpiredCidAndReadvertiseChunk(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	ttl := 3 * time.Second
	ip, err := ipfsip.NewIndexProviderWithNonceGen(context.Background(), mockEng, ttl, 2, datastore.NewMapDatastore(), testNonceGen)
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

	provide(t, c, ctx, testCid1)
	time.Sleep(2 * time.Second)
	provide(t, c, ctx, testCid2)
	time.Sleep(2 * time.Second)
	provide(t, c, ctx, testCid3)
}

func TestShouldRemoveCidsAsTheyExpire(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid3.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())))

	ttl := 1 * time.Second
	ip, err := ipfsip.NewIndexProviderWithNonceGen(context.Background(), mockEng, ttl, 1, datastore.NewMapDatastore(), testNonceGen)
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

	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid2)
	provide(t, c, ctx, testCid3)
	time.Sleep(1 * time.Second)
	provide(t, c, ctx, testCid3)
}

func TestRepublishingCidShouldUpdateItsExpiryDate(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid3.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())))

	ttl := 1 * time.Second
	ip, err := ipfsip.NewIndexProviderWithNonceGen(context.Background(), mockEng, ttl, 1, datastore.NewMapDatastore(), testNonceGen)
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

	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid2)
	time.Sleep(1 * time.Second)
	provide(t, c, ctx, testCid3)
}

func TestShouldNotReadvertiseChunkIfAllItsCidsExpired(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	ttl := 1 * time.Second
	ip, err := ipfsip.NewIndexProviderWithNonceGen(context.Background(), mockEng, ttl, 1, datastore.NewMapDatastore(), testNonceGen)
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

	provide(t, c, ctx, testCid1)
	time.Sleep(2 * time.Second)
	provide(t, c, ctx, testCid2)
}

func TestProvidingSameCidMultipleTimesShouldntAffectTheCurrentChunk(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	ttl := 1 * time.Hour
	ip, err := ipfsip.NewIndexProvider(context.Background(), mockEng, ttl, 2, datastore.NewMapDatastore())
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

	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid1)
}

func TestShouldInitialiseFromDatastore(t *testing.T) {
	priv, pID := generateKeyAndIdentity(t)

	ctx := context.Background()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	testCid4 := newCid("test4")

	mc := gomock.NewController(t)
	mockEng := mock_engine.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(generateContextID([]string{testCid3.String(), testCid4.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())))

	ttl := time.Second
	ds := datastore.NewMapDatastore()
	ip1, err := ipfsip.NewIndexProviderWithNonceGen(context.Background(), mockEng, ttl, 2, ds, testNonceGen)
	if err != nil {
		t.Fatal(err)
	}

	c, s := createClientAndServer(t, ip1, &client.Provider{
		Peer: peer.AddrInfo{
			ID:    pID,
			Addrs: []multiaddr.Multiaddr{},
		},
		ProviderProto: []client.TransferProtocol{},
	}, priv)
	defer s.Close()

	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid2)
	provide(t, c, ctx, testCid3)
	provide(t, c, ctx, testCid4)

	s.Close()

	ip2, err := ipfsip.NewIndexProviderWithNonceGen(context.Background(), mockEng, ttl, 2, ds, testNonceGen)
	if err != nil {
		t.Fatal(err)
	}
	c, s = createClientAndServer(t, ip2, &client.Provider{
		Peer: peer.AddrInfo{
			ID:    pID,
			Addrs: []multiaddr.Multiaddr{},
		},
		ProviderProto: []client.TransferProtocol{},
	}, priv)
	defer s.Close()

	time.Sleep(2 * time.Second)
	provide(t, c, ctx, testCid4)
}

func provide(t *testing.T, cc *client.Client, ctx context.Context, c cid.Cid) time.Duration {
	rc, err := cc.Provide(ctx, c, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return rc
}

func generateContextID(cids []string, nonce []byte) []byte {
	sort.Strings(cids)
	hasher := sha256.New()
	for _, c := range cids {
		hasher.Write([]byte(c))
	}
	hasher.Write(nonce)
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
