package sessions

import (
	"fmt"
	"sync"
)

var _ TheSessionsProvider = (*memProvider)(nil)

func RegisterMemSessionProvider() {
	Register("mem", NewMemProvider())
}

func UnRegisterMemSessionProvider() {
	Unregister("mem")
}

type memProvider struct {
	mu      sync.RWMutex
	sessMap map[string]*Session
}

func NewMemProvider() *memProvider {
	return &memProvider{
		sessMap: make(map[string]*Session),
	}
}

func (m *memProvider) New(id string) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sessMap[id] = &Session{id: id}
	return m.sessMap[id], nil
}

func (m *memProvider) Get(id string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sess, ok := m.sessMap[id]
	if !ok {
		return nil, fmt.Errorf("sessions/mem_provide/Get: No session found for key %s", id)
	}

	return sess, nil
}

func (m *memProvider) Del(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessMap, id)
}

func (m *memProvider) Save(id string) error {
	return nil
}

func (m *memProvider) Count() int {
	return len(m.sessMap)
}

func (m *memProvider) Close() error {
	m.sessMap = make(map[string]*Session)
	return nil
}
