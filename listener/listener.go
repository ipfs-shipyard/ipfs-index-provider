package listener

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"
	logging "github.com/ipfs/go-log/v2"

	// leveldb "github.com/ipfs/go-ds-leveldb"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

var log = logging.Logger("listener")
var bitswapMetadata = metadata.New(metadata.Bitswap{})

type DelegatedRoutingIndexProvider struct {
	e EngineProxy
	// ds      datastore.Datastore
	cidExpiryPeriod time.Duration
	chunker         *cidsChunker
	cidNodes        map[cid.Cid]*cidNode
	first           *cidNode
	last            *cidNode
}

func NewIndexProvider(e EngineProxy, cidExpiryPeriod time.Duration, cidsPerChunk int, cidCleanUpInterval time.Duration) (*DelegatedRoutingIndexProvider, error) {
	// err = checkWritable(dir)
	// if err != nil {
	// 	return nil, err
	// }
	// ds, err := leveldb.NewDatastore(dir, nil)
	// if err != nil {
	// 	return nil, err
	// }

	indexProvider := &DelegatedRoutingIndexProvider{e: e,
		cidExpiryPeriod: cidExpiryPeriod,
		cidNodes:        make(map[cid.Cid]*cidNode),
		chunker: &cidsChunker{
			cidsPerChunk:     cidsPerChunk,
			current:          &cidsChunk{},
			chunkByContextId: make(map[string]*cidsChunk),
			chunkByCid:       make(map[cid.Cid]*cidsChunk),
		},
	}

	e.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {
		chunk := indexProvider.chunker.chunkByContextId[string(contextID)]
		if chunk == nil {
			log.Error("MultihasLister couldn't find chunk for contextID %s", string(contextID))
			return nil, errors.New("MultihasLister couldn't find chunk for contextID")
		}
		var mhs []multihash.Multihash
		for _, c := range chunk.cids {
			mhs = append(mhs, c.Hash())
		}
		return provider.SliceMultihashIterator(mhs), nil
	})

	return indexProvider, nil
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
		n := d.cidNodes[pr.Key]
		if n == nil {
			n = &cidNode{timestamp: pr.Timestamp, c: pr.Key, provider: pr.Peer, next: d.first}
			d.cidNodes[pr.Key] = n
			d.addFirst(n)
			d.chunkCid(ctx, pr.Key)
		} else {
			n.timestamp = pr.Timestamp
			if n.prev != nil {
				n.prev.next = n.next
			}
			if n.next != nil {
				n.next.prev = n.prev
			}
			d.addFirst(n)
		}
		d.removeExpiredCids(ctx)

		ch <- client.ProvideAsyncResult{AdvisoryTTL: time.Duration(d.cidExpiryPeriod)}
		close(ch)
	}()
	return ch, nil
}

func (d *DelegatedRoutingIndexProvider) addFirst(n *cidNode) {
	if d.first != nil {
		n.next = d.first
		d.first.prev = n
	}
	d.first = n
	if d.last == nil {
		d.last = n
	}
}

func (d *DelegatedRoutingIndexProvider) removeLast() *cidNode {
	if d.last == nil {
		return nil
	}
	removed := d.last
	d.last = d.last.prev
	if d.last != nil {
		d.last.next = nil
	} else {
		d.first = nil
	}
	return removed
}

func (d *DelegatedRoutingIndexProvider) removeExpiredCids(ctx context.Context) {
	node := d.last
	t := time.Now().UnixMilli()
	chunksToRepublish := make(map[string]*cidsChunk)
	expiredCids := make(map[cid.Cid]bool)
	for {
		if node == nil || time.Duration(t-node.timestamp) <= d.cidExpiryPeriod {
			break
		}
		removedNode := d.removeLast()
		node = d.last

		if removedNode != nil {
			chunk := d.chunker.chunkByCid[removedNode.c]
			expiredCids[removedNode.c] = true
			chunksToRepublish[string(chunk.contextID)] = chunk
		}
	}

	for _, chunk := range chunksToRepublish {
		newChunk := &cidsChunk{}
		for _, c := range chunk.cids {
			if expiredCids[c] {
				continue
			}
			newChunk.cids = append(newChunk.cids, c)
			d.chunker.chunkByCid[c] = newChunk
		}
		chunk.removed = true
		newChunk.contextID = generateContextID(newChunk.cids)
		d.chunker.chunkByContextId[string(newChunk.contextID)] = newChunk
		d.e.NotifyRemove(ctx, chunk.contextID)
		d.e.NotifyPut(ctx, newChunk.contextID, bitswapMetadata)
	}
}

type cidNode struct {
	timestamp int64
	c         cid.Cid
	provider  peer.AddrInfo
	prev      *cidNode
	next      *cidNode
}

type cidsChunker struct {
	cidsPerChunk     int
	current          *cidsChunk
	chunkByContextId map[string]*cidsChunk
	chunkByCid       map[cid.Cid]*cidsChunk
}

type cidsChunk struct {
	removed   bool
	contextID []byte
	cids      []cid.Cid
}

func (d *DelegatedRoutingIndexProvider) chunkCid(ctx context.Context, c cid.Cid) error {
	d.chunker.current.cids = append(d.chunker.current.cids, c)

	if len(d.chunker.current.cids) == d.chunker.cidsPerChunk {
		d.chunker.current.contextID = generateContextID(d.chunker.current.cids)
		d.chunker.chunkByContextId[string(d.chunker.current.contextID)] = d.chunker.current
		for _, c := range d.chunker.current.cids {
			d.chunker.chunkByCid[c] = d.chunker.current
		}
		toPublish := d.chunker.current
		d.chunker.current = &cidsChunk{}

		//TODO: record proper metadata
		d.e.NotifyPut(ctx, toPublish.contextID, bitswapMetadata)
	}

	return nil
}

func generateContextID(cids []cid.Cid) []byte {
	hasher := sha256.New()
	for _, c := range cids {
		hasher.Write(c.Bytes())
	}
	return hasher.Sum(nil)
}
