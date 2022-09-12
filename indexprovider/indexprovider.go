package indexprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"

	dsq "github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-delegated-routing/client"
	logging "github.com/ipfs/go-log/v2"

	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("ipfs-index-provider")
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

	e.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {
		chunk := indexProvider.chunkByContextId[string(contextID)]
		if chunk == nil {
			log.Error("MultihasLister couldn't find chunk for contextID %s", string(contextID))
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

		d.chunkByContextId[string(chunk.ContextID)] = chunk
		for k := range chunk.Cids {
			d.chunkByCid[k] = chunk
		}
	}
	return nil
}

func (DelegatedRoutingIndexProvider) GetIPNS(ctx context.Context, id []byte) (<-chan client.GetIPNSAsyncResult, error) {
	ch := make(chan client.GetIPNSAsyncResult)
	go func() {
		// Not implemented
		ch <- client.GetIPNSAsyncResult{Record: nil}
		close(ch)
	}()
	return ch, nil
}

func (DelegatedRoutingIndexProvider) PutIPNS(ctx context.Context, id []byte, record []byte) (<-chan client.PutIPNSAsyncResult, error) {
	ch := make(chan client.PutIPNSAsyncResult)
	go func() {
		// Not implemented
		ch <- client.PutIPNSAsyncResult{}
		close(ch)
	}()
	return ch, nil
}

func (DelegatedRoutingIndexProvider) FindProviders(ctx context.Context, key cid.Cid) (<-chan client.FindProvidersAsyncResult, error) {
	ch := make(chan client.FindProvidersAsyncResult)
	go func() {
		// Not implemented
		ch <- client.FindProvidersAsyncResult{AddrInfo: nil}
		close(ch)
	}()
	return ch, nil
}

func (d *DelegatedRoutingIndexProvider) Provide(ctx context.Context, pr *client.ProvideRequest) (<-chan client.ProvideAsyncResult, error) {
	ch := make(chan client.ProvideAsyncResult)

	go func() {
		n := d.nodeByCid[pr.Key]
		var err error
		if n == nil {
			n = &cidNode{timestamp: time.UnixMilli(pr.Timestamp * 1000), c: pr.Key, next: d.firstNode}
			err = d.ds.Put(ctx, timestampByCidKey(pr.Key), int64ToBytes(n.timestamp.UnixMilli()))
			if err == nil {
				d.nodeByCid[pr.Key] = n
				d.addFirstNode(n)
				err = d.addCidToChunk(ctx, pr.Key)
			} else {
				log.Error(fmt.Sprintf("Error persisting timestamp for node %s", n.c), err)
			}
		} else {
			n.timestamp = time.UnixMilli(pr.Timestamp * 1000)
			err = d.ds.Put(ctx, timestampByCidKey(pr.Key), int64ToBytes(n.timestamp.UnixMilli()))
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
				if _, ok := d.chunkByCid[pr.Key]; !ok {
					err = d.addCidToChunk(ctx, pr.Key)
				}
			} else {
				log.Error(fmt.Sprintf("Error persisting timestamp for node %s", n.c), err)
			}
		}
		d.removeExpiredCids(ctx)

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
		if chunk == nil {
			continue
		}

		expiredCids[removedNode.c] = true
		chunksToRepublish[string(chunk.ContextID)] = chunk

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
			d.chunkByContextId[string(newChunk.ContextID)] = newChunk
			newChunk.ContextID = generateContextID(newChunk.Cids, d.nonceGen())
			err = d.notifyPutAndPersist(ctx, newChunk)
			if err != nil {
				return err
			}
		}
		// cleaning up expired cids from the datastore
		for _, c := range cidsToCleanUp {
			err = d.ds.Delete(ctx, timestampByCidKey(c))
			if err != nil {
				log.Error("Error cleaning up timestamp by cid index. Continuing.", err)
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
		log.Error(fmt.Sprintf("Error serializing the chunk %s", string(chunk.ContextID)), err)
		return err
	}

	err = d.ds.Put(ctx, chunkByContextIDKey(chunk.ContextID), b.Bytes())
	if err != nil {
		log.Error(fmt.Sprintf("Error persisting the chunk %s", string(chunk.ContextID)), err)
		return err
	}

	return nil
}

func (d *DelegatedRoutingIndexProvider) notifyRemoveAndPersist(ctx context.Context, chunk *cidsChunk) error {
	_, err := d.e.NotifyRemove(ctx, chunk.ContextID)
	if err != nil {
		log.Error(fmt.Sprintf("Error invoking NotifyRemove for the chunk %s", string(chunk.ContextID)), err)
		return err
	}
	err = d.persistChunk(ctx, chunk)
	if err != nil {
		return err
	}
	return nil
}

func (d *DelegatedRoutingIndexProvider) notifyPutAndPersist(ctx context.Context, chunk *cidsChunk) error {
	_, err := d.e.NotifyPut(ctx, chunk.ContextID, bitswapMetadata)
	if err != nil {
		log.Error(fmt.Sprintf("Error invoking NotifyPut for the chunk %s", string(chunk.ContextID)), err)
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
	Removed   bool
	ContextID []byte
	Cids      map[cid.Cid]bool
}

func (d *DelegatedRoutingIndexProvider) addCidToChunk(ctx context.Context, c cid.Cid) error {
	d.currentChunk.Cids[c] = true

	if len(d.currentChunk.Cids) == d.chunkSize {
		d.currentChunk.ContextID = generateContextID(d.currentChunk.Cids, d.nonceGen())
		d.chunkByContextId[string(d.currentChunk.ContextID)] = d.currentChunk
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
	return datastore.NewKey(chunkByContextIdIndexPrefix + string(contextID))
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
