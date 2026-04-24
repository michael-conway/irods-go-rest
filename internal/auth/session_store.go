package auth

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
)

type Session struct {
	ID        string
	Principal Principal
	Token     Token
}

type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]Session
}

func NewSessionStore() *SessionStore {
	return &SessionStore{
		sessions: map[string]Session{},
	}
}

func (s *SessionStore) Create(principal Principal, token Token) (Session, error) {
	sessionID, err := randomSessionID(32)
	if err != nil {
		return Session{}, err
	}

	session := Session{
		ID:        sessionID,
		Principal: principal,
		Token:     token,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[sessionID] = session

	return session, nil
}

func (s *SessionStore) Get(sessionID string) (Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	session, ok := s.sessions[sessionID]
	return session, ok
}

func (s *SessionStore) Delete(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, sessionID)
}

func randomSessionID(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate session id: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(buf), nil
}
