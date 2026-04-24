package auth

import "context"

type principalContextKey struct{}
type ticketContextKey struct{}
type basicPasswordContextKey struct{}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

func WithTicket(ctx context.Context, ticket string) context.Context {
	return context.WithValue(ctx, ticketContextKey{}, ticket)
}

func TicketFromContext(ctx context.Context) (string, bool) {
	ticket, ok := ctx.Value(ticketContextKey{}).(string)
	return ticket, ok
}

func WithBasicPassword(ctx context.Context, password string) context.Context {
	return context.WithValue(ctx, basicPasswordContextKey{}, password)
}

func BasicPasswordFromContext(ctx context.Context) (string, bool) {
	password, ok := ctx.Value(basicPasswordContextKey{}).(string)
	return password, ok
}
