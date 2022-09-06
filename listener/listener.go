package listener

import (
	"context"
	"crypto/rand"
	"errors"
	"time"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-delegated-routing/client"

	// leveldb "github.com/ipfs/go-ds-leveldb"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

type DelegatedRoutingIndexProvider struct {
	e *engine.Engine
	// ds      datastore.Datastore
	ttl     int64
	chunker *cidsChunker
	nodes   map[cid.Cid]*node
	first   *node
	last    *node
}

func NewIndexProvider(e *engine.Engine, ttl int64, cidsPerChunk int, contextIdLength int) (*DelegatedRoutingIndexProvider, error) {
	// err = checkWritable(dir)
	// if err != nil {
	// 	return nil, err
	// }
	// ds, err := leveldb.NewDatastore(dir, nil)
	// if err != nil {
	// 	return nil, err
	// }

	contextID, err := randomBytes(contextIdLength)
	if err != nil {
		return nil, err
	}

	indexProvider := &DelegatedRoutingIndexProvider{e: e,
		ttl:   ttl,
		nodes: make(map[cid.Cid]*node),
		chunker: &cidsChunker{
			cidsPerChunk:    cidsPerChunk,
			contextIdLength: contextIdLength,
			current:         &cidsChunk{contextID: contextID},
		},
	}

	e.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {
		chunk := indexProvider.chunker.chunkByCid[string(contextID)]
		if chunk == nil {
			return nil, errors.New("not found")
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
		n := d.nodes[pr.Key]
		if n == nil {
			n = &node{timestamp: pr.Timestamp, c: pr.Key, provider: pr.Peer, next: d.first}
			d.nodes[pr.Key] = n
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
		d.purgeExpired(ctx)
		ch <- client.ProvideAsyncResult{AdvisoryTTL: time.Duration(d.ttl)}
		close(ch)
	}()
	return ch, nil
}

func (d *DelegatedRoutingIndexProvider) addFirst(n *node) {
	if d.first != nil {
		n.next = d.first
		d.first.prev = n
	}
	d.first = n
	if d.last == nil {
		d.last = n
	}
}

func (d *DelegatedRoutingIndexProvider) removeLast() *node {
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

func (d *DelegatedRoutingIndexProvider) purgeExpired(ctx context.Context) {
	n := d.last
	t := time.Now().UnixMilli()
	for {
		if n == nil || t-n.timestamp <= d.ttl {
			break
		}
		r := d.removeLast()
		n = d.last

		if r != nil {
			// publish delete
		}
	}
}

type node struct {
	timestamp int64
	c         cid.Cid
	provider  peer.AddrInfo
	prev      *node
	next      *node
}

type cidsChunker struct {
	cidsPerChunk    int
	contextIdLength int
	current         *cidsChunk
	chunkByCid      map[string]*cidsChunk
}

type cidsChunk struct {
	contextID []byte
	cids      []cid.Cid
}

func (d *DelegatedRoutingIndexProvider) chunkCid(ctx context.Context, c cid.Cid) error {
	d.chunker.current.cids = append(d.chunker.current.cids, c)

	if len(d.chunker.current.cids) == d.chunker.cidsPerChunk {
		d.chunker.chunkByCid[string(d.chunker.current.contextID)] = d.chunker.current

		contextID, err := randomBytes(d.chunker.contextIdLength)
		if err != nil {
			return err
		}

		toPublish := d.chunker.current
		d.chunker.current = &cidsChunk{contextID: contextID}

		//TODO: record proper metadata
		d.e.NotifyPut(ctx, toPublish.contextID, metadata.New(metadata.Bitswap{}))
	}

	return nil
}

func randomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}
	return b, nil
}
