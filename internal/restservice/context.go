package restservice

import (
	"context"
	"fmt"

	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/requestctx"
)

type RequestContext struct {
	AuthScheme     string
	Principal      *auth.Principal
	BasicPassword  string
	Ticket         string
	RequestID      string
	RequestSource  string
	RequestActor   string
	IdempotencyKey string
}

func RequestContextFromContext(ctx context.Context) (*RequestContext, error) {
	if ctx == nil {
		return nil, fmt.Errorf("missing request context")
	}

	if ticket, ok := auth.TicketFromContext(ctx); ok && ticket != "" {
		requestContext := &RequestContext{
			AuthScheme: "bearer-ticket",
			Ticket:     ticket,
		}
		applyRequestMetadata(requestContext, ctx)
		return requestContext, nil
	}

	principal, principalOK := auth.PrincipalFromContext(ctx)
	if !principalOK {
		return nil, fmt.Errorf("missing authenticated principal")
	}

	requestContext := &RequestContext{
		AuthScheme: "bearer",
		Principal:  &principal,
	}

	if password, ok := auth.BasicPasswordFromContext(ctx); ok {
		requestContext.AuthScheme = "basic"
		requestContext.BasicPassword = password
	}

	applyRequestMetadata(requestContext, ctx)
	return requestContext, nil
}

func applyRequestMetadata(requestContext *RequestContext, ctx context.Context) {
	if requestContext == nil {
		return
	}

	metadata, ok := requestctx.MetadataFromContext(ctx)
	if !ok {
		return
	}

	requestContext.RequestID = metadata.RequestID
	requestContext.RequestSource = metadata.Source
	requestContext.RequestActor = metadata.Actor
	requestContext.IdempotencyKey = metadata.IdempotencyKey
}
