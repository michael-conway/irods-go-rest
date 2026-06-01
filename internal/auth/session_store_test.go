package auth

import (
	"testing"
	"time"
)

func TestSessionStoreExpiryUsesTokenTTL(t *testing.T) {
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	store := NewSessionStore(withSessionStoreNow(func() time.Time { return now }))

	session, err := store.Create(Principal{Subject: "user-123", Active: true}, Token{AccessToken: "token", ExpiresIn: 120})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	if got := session.ExpiresAt; !got.Equal(now.Add(120 * time.Second)) {
		t.Fatalf("expected session expiry from token ttl, got %s", got)
	}
}

func TestSessionStoreExpiryFallsBackToDefaultTTL(t *testing.T) {
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	store := NewSessionStore(
		WithSessionStoreDefaultTTL(10*time.Minute),
		withSessionStoreNow(func() time.Time { return now }),
	)

	session, err := store.Create(Principal{Subject: "user-123", Active: true}, Token{AccessToken: "token"})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	if got := session.ExpiresAt; !got.Equal(now.Add(10 * time.Minute)) {
		t.Fatalf("expected session expiry from default ttl, got %s", got)
	}
}

func TestSessionStoreGetEvictsExpiredSession(t *testing.T) {
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	current := now
	store := NewSessionStore(withSessionStoreNow(func() time.Time { return current }))

	session, err := store.Create(Principal{Subject: "user-123", Active: true}, Token{AccessToken: "token", ExpiresIn: 1})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	current = now.Add(2 * time.Second)
	if _, ok := store.Get(session.ID); ok {
		t.Fatal("expected expired session to be missing")
	}

	if count := store.Count(); count != 0 {
		t.Fatalf("expected expired session eviction, got count %d", count)
	}
}

func TestSessionStoreEvictsOldestAtCapacity(t *testing.T) {
	now := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)
	current := now
	store := NewSessionStore(
		WithSessionStoreMaxSessions(2),
		withSessionStoreNow(func() time.Time { return current }),
	)

	session1, err := store.Create(Principal{Subject: "user-1", Active: true}, Token{AccessToken: "token-1", ExpiresIn: 3600})
	if err != nil {
		t.Fatalf("create first session: %v", err)
	}
	current = current.Add(1 * time.Second)
	session2, err := store.Create(Principal{Subject: "user-2", Active: true}, Token{AccessToken: "token-2", ExpiresIn: 3600})
	if err != nil {
		t.Fatalf("create second session: %v", err)
	}
	current = current.Add(1 * time.Second)
	session3, err := store.Create(Principal{Subject: "user-3", Active: true}, Token{AccessToken: "token-3", ExpiresIn: 3600})
	if err != nil {
		t.Fatalf("create third session: %v", err)
	}

	if _, ok := store.Get(session1.ID); ok {
		t.Fatal("expected oldest session to be evicted")
	}
	if _, ok := store.Get(session2.ID); !ok {
		t.Fatal("expected second session to remain")
	}
	if _, ok := store.Get(session3.ID); !ok {
		t.Fatal("expected third session to remain")
	}
	if count := store.Count(); count != 2 {
		t.Fatalf("expected capacity-bound count 2, got %d", count)
	}
}
