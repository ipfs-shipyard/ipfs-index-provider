package indexprovider

import (
	"context"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type EngineProxy interface {
	RegisterMultihashLister(l provider.MultihashLister)
	NotifyPut(ctx context.Context, provider *peer.AddrInfo, contextID []byte, md metadata.Metadata) (cid.Cid, error)
	NotifyRemove(ctx context.Context, providerID peer.ID, contextID []byte) (cid.Cid, error)
	Start(ctx context.Context) error
	Shutdown() error
}
