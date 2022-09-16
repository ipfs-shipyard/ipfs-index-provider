package indexprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"math/rand"
	"sort"
	"time"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/peer"

	dsq "github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-delegated-routing/client"
	logging "github.com/ipfs/go-log/v2"

	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("ipfs-index-provider")
var lastSeenProviderInfo peer.AddrInfo = peer.AddrInfo{}
var bitswapMetadata = metadata.New(metadata.Bitswap{})

const chunkByContextIdIndexPrefix = "i/ccid/"
const timestampByCidIndexPrefix = "i/tc/"

type DelegatedRoutingIndexProvider struct {
	ds               datastore.Datastore
	e                EngineProxy
	cidExpiryPeriod  time.Duration
	chunkSize        int
	currentChunk     *cidsChunk
	chunkByContextId map[string]*cidsChunk
	chunkByCid       map[cid.Cid]*cidsChunk
	nodeByCid        map[cid.Cid]*cidNode
	firstNode        *cidNode
	lastNode         *cidNode
	nonceGen         func() []byte
}

func NewIndexProvider(ctx context.Context, e EngineProxy, cidExpiryPeriod time.Duration, chunkSize int, ds datastore.Datastore) (*DelegatedRoutingIndexProvider, error) {
	return NewIndexProviderWithNonceGen(ctx, e, cidExpiryPeriod, chunkSize, ds, func() []byte {
		nonce := make([]byte, 8)
		rand.Read(nonce)
		return nonce
	})
}

func NewIndexProviderWithNonceGen(ctx context.Context, e EngineProxy, cidExpiryPeriod time.Duration, chunkSize int, ds datastore.Datastore, nonceGen func() []byte) (*DelegatedRoutingIndexProvider, error) {

	indexProvider := &DelegatedRoutingIndexProvider{e: e,
		cidExpiryPeriod:  cidExpiryPeriod,
		chunkSize:        chunkSize,
		currentChunk:     &cidsChunk{Cids: make(map[cid.Cid]bool)},
		nodeByCid:        make(map[cid.Cid]*cidNode),
		chunkByContextId: make(map[string]*cidsChunk),
		chunkByCid:       make(map[cid.Cid]*cidsChunk),
		ds:               ds,
		nonceGen:         nonceGen,
	}

	e.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		chunk := indexProvider.chunkByContextId[contextIDToStr(contextID)]
		if chunk == nil {
			log.Error("MultihasLister couldn't find chunk for contextID %s", contextIDToStr(contextID))
			return nil, errors.New("MultihasLister couldn't find chunk for contextID")
		}
		var mhs []multihash.Multihash
		for c := range chunk.Cids {
			mhs = append(mhs, c.Hash())
		}
		return provider.SliceMultihashIterator(mhs), nil
	})

	indexProvider.initialiseFromTheDatastore(ctx)

	return indexProvider, nil
}

func (d *DelegatedRoutingIndexProvider) Start(ctx context.Context) error {
	return d.e.Start(ctx)
}

func (d *DelegatedRoutingIndexProvider) Shutdown() error {
	return d.e.Shutdown()
}

func (d *DelegatedRoutingIndexProvider) initialiseFromTheDatastore(ctx context.Context) error {
	// reading up timestamp by cid index from the datastore
	q := dsq.Query{Prefix: timestampByCidIndexPrefix}
	tcResults, err := d.ds.Query(ctx, q)
	if err != nil {
		log.Error("Error reading timestamp by cid index from the datastore", err)
		return err
	}
	defer tcResults.Close()
	var sortedNodes []*cidNode
	for r := range tcResults.Next() {
		if ctx.Err() != nil {
			log.Error("Received error from the context while initialising from the datastore", ctx.Err())
			return ctx.Err()
		}
		if r.Error != nil {
			log.Error("Error fetching datastore record", r.Error)
			return r.Error
		}

		timestamp := bytesToInt64(r.Value)
		cs := r.Key[len(timestampByCidIndexPrefix)+1:]
		c, err := cid.Parse(cs)
		if err != nil {
			log.Error("Error parsing cid datastore record", err)
			return err
		}

		sortedNodes = append(sortedNodes, &cidNode{timestamp: time.UnixMilli(timestamp), c: c})
	}

	sort.SliceStable(sortedNodes, func(i, j int) bool {
		return sortedNodes[i].timestamp.UnixMilli() < sortedNodes[j].timestamp.UnixMilli()
	})

	for i := range sortedNodes {
		n := sortedNodes[len(sortedNodes)-1-i]
		d.addFirstNode(n)
		d.nodeByCid[n.c] = n
	}

	// reading all chunks from the datastore
	q = dsq.Query{Prefix: chunkByContextIdIndexPrefix}
	ccResults, err := d.ds.Query(ctx, q)
	if err != nil {
		log.Error("Error reading from the datastore", err)
		return err
	}
	defer ccResults.Close()

	for r := range ccResults.Next() {
		if ctx.Err() != nil {
			log.Error("Received error from the context while reading the datastore", ctx.Err())
			return ctx.Err()
		}
		if r.Error != nil {
			log.Error("Error fetching datastore record", r.Error)
			return r.Error
		}

		chunk := &cidsChunk{Cids: make(map[cid.Cid]bool)}
		decoder := gob.NewDecoder(bytes.NewBuffer(r.Value))
		err = decoder.Decode(chunk)

		if err != nil {
			log.Error("Error deserialising record from the datastore", err)
			return err
		}

		d.chunkByContextId[contextIDToStr(chunk.ContextID)] = chunk
		for k := range chunk.Cids {
			d.chunkByCid[k] = chunk
		}
	}
	return nil
}

func (DelegatedRoutingIndexProvider) GetIPNS(ctx context.Context, id []byte) (<-chan client.GetIPNSAsyncResult, error) {
	log.Info("Received reframe:getIPNS request")
	ch := make(chan client.GetIPNSAsyncResult)
	go func() {
		// Not implemented
		ch <- client.GetIPNSAsyncResult{Record: nil}
		close(ch)
	}()
	return ch, nil
}

func (DelegatedRoutingIndexProvider) PutIPNS(ctx context.Context, id []byte, record []byte) (<-chan client.PutIPNSAsyncResult, error) {
	log.Info("Received reframe:putIPNS request")
	ch := make(chan client.PutIPNSAsyncResult)
	go func() {
		// Not implemented
		ch <- client.PutIPNSAsyncResult{}
		close(ch)
	}()
	return ch, nil
}

func (DelegatedRoutingIndexProvider) FindProviders(ctx context.Context, key cid.Cid) (<-chan client.FindProvidersAsyncResult, error) {
	log.Info("Received reframe:findProviders request")
	ch := make(chan client.FindProvidersAsyncResult)
	go func() {
		// Not implemented
		ch <- client.FindProvidersAsyncResult{AddrInfo: nil}
		close(ch)
	}()
	return ch, nil
}

func (d *DelegatedRoutingIndexProvider) Provide(ctx context.Context, pr *client.ProvideRequest) (<-chan client.ProvideAsyncResult, error) {
	log.Info("Received reframe:provide request")
	ch := make(chan client.ProvideAsyncResult)

	go func() {
		lastSeenProviderInfo.ID = pr.Provider.Peer.ID
		lastSeenProviderInfo.Addrs = pr.Provider.Peer.Addrs
		var err error

		for _, c := range pr.Key {
			n := d.nodeByCid[c]
			if n == nil {
				log.Info("Seeing cid=", c.String(), " for the first time")
				n = &cidNode{timestamp: time.UnixMilli(pr.Timestamp * 1000), c: c, next: d.firstNode}
				err = d.ds.Put(ctx, timestampByCidKey(c), int64ToBytes(n.timestamp.Add(d.cidExpiryPeriod).UnixMilli()))
				if err == nil {
					d.nodeByCid[c] = n
					d.addFirstNode(n)
					err = d.addCidToChunk(ctx, c)
					if err != nil {
						break
					}
				} else {
					log.Error("Error persisting timestamp for cid ", n.c.String(), ", err=", err)
					break
				}
			} else {
				n.timestamp = time.UnixMilli(pr.Timestamp * 1000)
				log.Info("Already saw cid=", c.String(), ", updating timestamp to %s", n.timestamp.String())
				err = d.ds.Put(ctx, timestampByCidKey(c), int64ToBytes(n.timestamp.UnixMilli()))
				if err == nil {
					// moving the node to the beginning of the linked list
					if n.prev != nil {
						n.prev.next = n.next
					} else {
						// that was the first node
						d.firstNode = n.next
					}
					if n.next != nil {
						n.next.prev = n.prev
					} else {
						// that was the last node
						d.lastNode = n.prev
					}
					d.addFirstNode(n)

					// if no existing chunk has been found for the cid - adding it to the current one
					if _, ok := d.chunkByCid[c]; !ok {
						err = d.addCidToChunk(ctx, c)
						if err != nil {
							break
						}
					}
				} else {
					log.Error("Error persisting timestamp for cid=", n.c.String(), ", err=", err)
					break
				}
			}
			d.removeExpiredCids(ctx)
		}
		ch <- client.ProvideAsyncResult{AdvisoryTTL: time.Duration(d.cidExpiryPeriod), Err: err}
		close(ch)
	}()
	return ch, nil
}

func (d *DelegatedRoutingIndexProvider) addFirstNode(n *cidNode) {
	if d.firstNode != nil {
		n.next = d.firstNode
		d.firstNode.prev = n
	}
	d.firstNode = n
	n.prev = nil
	if d.lastNode == nil {
		d.lastNode = n
	}
}

func (d *DelegatedRoutingIndexProvider) removeLastNode() *cidNode {
	if d.lastNode == nil {
		return nil
	}
	removed := d.lastNode
	d.lastNode = d.lastNode.prev
	if d.lastNode != nil {
		d.lastNode.next = nil
	} else {
		d.firstNode = nil
	}
	if removed != nil {
		delete(d.nodeByCid, removed.c)
	}
	return removed
}

func (d *DelegatedRoutingIndexProvider) removeExpiredCids(ctx context.Context) error {
	node := d.lastNode
	t := time.Now()
	chunksToRepublish := make(map[string]*cidsChunk)
	expiredCids := make(map[cid.Cid]bool)
	// find cids that have expired with their respective chunks
	for {
		if node == nil {
			break
		}

		if t.UnixMilli()-node.timestamp.UnixMilli() <= d.cidExpiryPeriod.Milliseconds() {
			break
		}

		removedNode := d.removeLastNode()

		chunk := d.chunkByCid[removedNode.c]
		if chunk != nil {
			chunksToRepublish[contextIDToStr(chunk.ContextID)] = chunk
			expiredCids[removedNode.c] = true
			log.Info("cid=", removedNode.c.String(), ", timestamp=", removedNode.timestamp.String(), "chunk=", contextIDToStr(chunk.ContextID), " has expired")
		}

		node = d.lastNode
	}

	// remove old chunks and generate new chunks less the expired cids
	for _, chunk := range chunksToRepublish {
		newChunk := &cidsChunk{Cids: make(map[cid.Cid]bool)}
		var cidsToCleanUp []cid.Cid
		for c := range chunk.Cids {
			if expiredCids[c] {
				cidsToCleanUp = append(cidsToCleanUp, c)
				continue
			}
			newChunk.Cids[c] = true
			d.chunkByCid[c] = newChunk
		}
		chunk.Removed = true
		err := d.notifyRemoveAndPersist(ctx, chunk)
		if err != nil {
			return err
		}
		// only generating a new chunk if it has some cids left in it
		if len(newChunk.Cids) > 0 {
			d.chunkByContextId[contextIDToStr(newChunk.ContextID)] = newChunk
			newChunk.ContextID = generateContextID(newChunk.Cids, d.nonceGen())
			newChunk.Provider = chunk.Provider
			err = d.notifyPutAndPersist(ctx, newChunk)
			if err != nil {
				return err
			}
		}
		// cleaning up expired cids from the datastore
		for _, c := range cidsToCleanUp {
			err = d.ds.Delete(ctx, timestampByCidKey(c))
			if err != nil {
				log.Error("Error cleaning up timestamp by cid index. Continuing. err=", err)
			}
		}
	}

	return nil
}

func (d *DelegatedRoutingIndexProvider) persistChunk(ctx context.Context, chunk *cidsChunk) error {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(chunk)
	if err != nil {
		log.Error("Error serializing the chunk=", contextIDToStr(chunk.ContextID), ", err=", err)
		return err
	}

	err = d.ds.Put(ctx, chunkByContextIDKey(chunk.ContextID), b.Bytes())
	if err != nil {
		log.Error("Error persisting the chunk=", contextIDToStr(chunk.ContextID), ", err=", err)
		return err
	}

	return nil
}

func (d *DelegatedRoutingIndexProvider) notifyRemoveAndPersist(ctx context.Context, chunk *cidsChunk) error {
	log.Info("Notifying Remove for chunk ", contextIDToStr(chunk.ContextID))
	_, err := d.e.NotifyRemove(ctx, chunk.Provider, chunk.ContextID)
	if err != nil {
		log.Error("Error invoking NotifyRemove for the chunk=", contextIDToStr(chunk.ContextID), ", err=", err)
		return err
	}
	err = d.persistChunk(ctx, chunk)
	if err != nil {
		return err
	}
	return nil
}

func (d *DelegatedRoutingIndexProvider) notifyPutAndPersist(ctx context.Context, chunk *cidsChunk) error {
	log.Info("Notifying Put for chunk=", contextIDToStr(chunk.ContextID))
	addrs, err := stringsToAddrs(chunk.Addrs)
	if err != nil {
		log.Error("Error while parsing multiaddresses from chunk.", err)
		return err
	}
	_, err = d.e.NotifyPut(ctx, &peer.AddrInfo{ID: chunk.Provider, Addrs: addrs}, chunk.ContextID, bitswapMetadata)
	if err != nil {
		log.Error("Error invoking NotifyPut for the chunk=", contextIDToStr(chunk.ContextID), ", err=", err)
		return err
	}
	err = d.persistChunk(ctx, chunk)
	if err != nil {
		return err
	}

	return nil
}

type cidNode struct {
	timestamp time.Time
	c         cid.Cid
	prev      *cidNode
	next      *cidNode
}
type cidsChunk struct {
	Provider  peer.ID
	Addrs     []string
	Removed   bool
	ContextID []byte
	Cids      map[cid.Cid]bool
}

func contextIDToStr(contextID []byte) string {
	return base64.StdEncoding.EncodeToString(contextID)
}

func (d *DelegatedRoutingIndexProvider) addCidToChunk(ctx context.Context, c cid.Cid) error {
	d.currentChunk.Cids[c] = true

	if len(d.currentChunk.Cids) == d.chunkSize {
		d.currentChunk.ContextID = generateContextID(d.currentChunk.Cids, d.nonceGen())
		d.currentChunk.Provider = lastSeenProviderInfo.ID
		d.currentChunk.Addrs = addrsToStrings(lastSeenProviderInfo.Addrs)
		d.chunkByContextId[contextIDToStr(d.currentChunk.ContextID)] = d.currentChunk
		for c := range d.currentChunk.Cids {
			d.chunkByCid[c] = d.currentChunk
		}
		toPublish := d.currentChunk
		d.currentChunk = &cidsChunk{Cids: make(map[cid.Cid]bool)}

		err := d.notifyPutAndPersist(ctx, toPublish)
		if err != nil {
			return err
		}
	}

	return nil
}

func generateContextID(cidsMap map[cid.Cid]bool, nonce []byte) []byte {
	cids := make([]string, len(cidsMap))
	i := 0
	for k := range cidsMap {
		cids[i] = k.String()
		i++
	}
	sort.Strings(cids)

	hasher := sha256.New()
	for _, c := range cids {
		hasher.Write([]byte(c))
	}
	hasher.Write(nonce)
	return hasher.Sum(nil)
}

func chunkByContextIDKey(contextID []byte) datastore.Key {
	return datastore.NewKey(chunkByContextIdIndexPrefix + contextIDToStr(contextID))
}

func timestampByCidKey(c cid.Cid) datastore.Key {
	return datastore.NewKey(timestampByCidIndexPrefix + c.String())
}

func int64ToBytes(i int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func bytesToInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b))
}

func addrsToStrings(addrs []multiaddr.Multiaddr) []string {
	s := make([]string, len(addrs))
	for i, a := range addrs {
		s[i] = a.String()
	}
	return s
}

func stringsToAddrs(strs []string) ([]multiaddr.Multiaddr, error) {
	addrs := make([]multiaddr.Multiaddr, len(strs))
	for i, s := range strs {
		a, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			return nil, err
		}
		addrs[i] = a
	}
	return addrs, nil
}
