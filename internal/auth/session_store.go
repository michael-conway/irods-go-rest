package auth

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"time"
)

type Session struct {
	ID        string
	Principal Principal
	Token     Token
	CreatedAt time.Time
	ExpiresAt time.Time
}

type sessionStoreConfig struct {
	defaultTTL  time.Duration
	maxSessions int
	now         func() time.Time
}

func defaultSessionStoreConfig() sessionStoreConfig {
	return sessionStoreConfig{
		defaultTTL:  15 * time.Minute,
		maxSessions: 1024,
		now:         time.Now,
	}
}

type SessionStoreOption func(*sessionStoreConfig)

func WithSessionStoreDefaultTTL(ttl time.Duration) SessionStoreOption {
	return func(cfg *sessionStoreConfig) {
		if ttl > 0 {
			cfg.defaultTTL = ttl
		}
	}
}

func WithSessionStoreMaxSessions(maxSessions int) SessionStoreOption {
	return func(cfg *sessionStoreConfig) {
		if maxSessions > 0 {
			cfg.maxSessions = maxSessions
		}
	}
}

func withSessionStoreNow(now func() time.Time) SessionStoreOption {
	return func(cfg *sessionStoreConfig) {
		if now != nil {
			cfg.now = now
		}
	}
}

type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]Session
	config   sessionStoreConfig
}

func NewSessionStore(options ...SessionStoreOption) *SessionStore {
	cfg := defaultSessionStoreConfig()
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}

	return &SessionStore{
		sessions: map[string]Session{},
		config:   cfg,
	}
}

func (s *SessionStore) Create(principal Principal, token Token) (Session, error) {
	sessionID, err := randomSessionID(32)
	if err != nil {
		return Session{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.config.now()

	// Trim stale sessions and enforce bounded memory before adding a new entry.
	s.evictExpiredLocked(now)
	s.evictToCapacityLocked()

	session := Session{
		ID:        sessionID,
		Principal: principal,
		Token:     token,
		CreatedAt: now,
		ExpiresAt: s.sessionExpiryLocked(now, token),
	}
	s.sessions[sessionID] = session

	return session, nil
}

func (s *SessionStore) Get(sessionID string) (Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	session, ok := s.sessions[sessionID]
	if !ok {
		return Session{}, false
	}

	if s.isExpiredLocked(session, s.config.now()) {
		delete(s.sessions, sessionID)
		return Session{}, false
	}

	return session, ok
}

func (s *SessionStore) Delete(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sessionID)
}

func (s *SessionStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sessions)
}

func (s *SessionStore) sessionExpiryLocked(now time.Time, token Token) time.Time {
	if token.ExpiresIn > 0 {
		return now.Add(time.Duration(token.ExpiresIn) * time.Second)
	}

	return now.Add(s.config.defaultTTL)
}

func (s *SessionStore) isExpiredLocked(session Session, now time.Time) bool {
	return !session.ExpiresAt.IsZero() && !session.ExpiresAt.After(now)
}

func (s *SessionStore) evictExpiredLocked(now time.Time) {
	for id, session := range s.sessions {
		if s.isExpiredLocked(session, now) {
			delete(s.sessions, id)
		}
	}
}

func (s *SessionStore) evictToCapacityLocked() {
	for len(s.sessions) >= s.config.maxSessions {
		oldestID := ""
		var oldestCreatedAt time.Time

		for id, session := range s.sessions {
			if oldestID == "" || session.CreatedAt.Before(oldestCreatedAt) {
				oldestID = id
				oldestCreatedAt = session.CreatedAt
			}
		}

		if oldestID == "" {
			return
		}
		delete(s.sessions, oldestID)
	}
}

func randomSessionID(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate session id: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(buf), nil
}
