package restservice

import (
	"context"
	"fmt"

	"github.com/michael-conway/irods-go-rest/internal/auth"
)

type RequestContext struct {
	AuthScheme    string
	Principal     *auth.Principal
	BasicPassword string
	Ticket        string
}

func RequestContextFromContext(ctx context.Context) (*RequestContext, error) {
	if ctx == nil {
		return nil, fmt.Errorf("missing request context")
	}

	if ticket, ok := auth.TicketFromContext(ctx); ok && ticket != "" {
		return &RequestContext{
			AuthScheme: "bearer-ticket",
			Ticket:     ticket,
		}, nil
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

	return requestContext, nil
}
