package requestctx

import (
	"context"
	"strings"
)

type Metadata struct {
	RequestID      string
	Source         string
	Actor          string
	IdempotencyKey string
}

type metadataContextKey struct{}

func WithMetadata(ctx context.Context, metadata Metadata) context.Context {
	return context.WithValue(ctx, metadataContextKey{}, normalizeMetadata(metadata))
}

func MetadataFromContext(ctx context.Context) (Metadata, bool) {
	if ctx == nil {
		return Metadata{}, false
	}

	metadata, ok := ctx.Value(metadataContextKey{}).(Metadata)
	return metadata, ok
}

func normalizeMetadata(metadata Metadata) Metadata {
	return Metadata{
		RequestID:      strings.TrimSpace(metadata.RequestID),
		Source:         strings.TrimSpace(metadata.Source),
		Actor:          strings.TrimSpace(metadata.Actor),
		IdempotencyKey: strings.TrimSpace(metadata.IdempotencyKey),
	}
}
