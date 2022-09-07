package listener

import (
	"context"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
)

type EngineProxy interface {
	RegisterMultihashLister(l provider.MultihashLister)
	NotifyPut(ctx context.Context, contextID []byte, md metadata.Metadata) (cid.Cid, error)
	NotifyRemove(ctx context.Context, contextID []byte) (cid.Cid, error)
}
