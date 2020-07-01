package sessions

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

var (
	//ErrSessionsProviderNotFound = errors.New("session: Session provider not found")
	//ErrKeyNotAvailable          = errors.New("session: Not item found for key")

	providers = make(map[string]TheSessionsProvider)
)

type TheSessionsProvider interface {
	New(id string) (*Session, error)
	Get(id string) (*Session, error)
	Del(id string)
	Save(id string) error
	Count() int
	Close() error
}

// Register makes a session provider available by the provided name.
// If a Register is called twice with the same name or if the driver is nil,
// it panics.
func Register(name string, provider TheSessionsProvider) {
	if provider == nil {
		panic("session_provider: Register provider is nil")
	}

	if _, dup := providers[name]; dup {
		panic("session_provider: Register called twice for provider " + name)
	}

	providers[name] = provider
}

func Unregister(name string) {
	delete(providers, name)
}

type Manager struct {
	tsp TheSessionsProvider
}

func NewManager(providerName string) (*Manager, error) {
	p, ok := providers[providerName]
	if !ok {
		return nil, fmt.Errorf("session_provider: unknown provider %q", providerName)
	}

	return &Manager{tsp: p}, nil
}

func (m *Manager) New(id string) (*Session, error) {
	if id == "" {
		id = m.sessionId()
	}
	return m.tsp.New(id)
}

func (m *Manager) Get(id string) (*Session, error) {
	return m.tsp.Get(id)
}

func (m *Manager) Del(id string) {
	m.tsp.Del(id)
}

func (m *Manager) Save(id string) error {
	return m.tsp.Save(id)
}

func (m *Manager) Count() int {
	return m.tsp.Count()
}

func (m *Manager) Close() error {
	return m.tsp.Close()
}

func (m *Manager) sessionId() string {
	b := make([]byte, 15)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return base64.URLEncoding.EncodeToString(b)
}
