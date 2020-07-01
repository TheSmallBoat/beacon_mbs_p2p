package topics

import (
	"fmt"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	// MWC is the multi-level wildcard
	MWC = "#"

	// SWC is the single level wildcard
	SWC = "+"

	// SEP is the topic level separator
	SEP = "/"

	// SYS is the starting character of the system level topics
	SYS = "$"

	// Both wildcards
	_WC = "#+"
)

var (
	providers = make(map[string]TheTopicsProvider)
)

type TheTopicsProvider interface {
	Subscribe(topic []byte, qos byte, subscriber interface{}) (byte, error)
	Unsubscribe(topic []byte, subscriber interface{}) error
	Subscribers(topic []byte, qos byte, subList *[]interface{}, qosList *[]byte) error
	Retain(message *packets.PublishPacket) error
	Retained(topic []byte, messages *[]*packets.PublishPacket) error
	Close() error
}

func Register(name string, provider TheTopicsProvider) {
	if provider == nil {
		panic("topic_provider: Register provider is nil")
	}

	if _, dup := providers[name]; dup {
		panic("topic_provider: Register called twice for provider " + name)
	}

	providers[name] = provider
}

func Unregister(name string) {
	delete(providers, name)
}

type Manager struct {
	ttp TheTopicsProvider
}

func NewManager(providerName string) (*Manager, error) {
	p, ok := providers[providerName]
	if !ok {
		return nil, fmt.Errorf("topic_provider: unknown provider %q", providerName)
	}

	return &Manager{ttp: p}, nil
}

func (m *Manager) Subscribe(topic []byte, qos byte, subscriber interface{}) (byte, error) {
	return m.ttp.Subscribe(topic, qos, subscriber)
}

func (m *Manager) Unsubscribe(topic []byte, subscriber interface{}) error {
	return m.ttp.Unsubscribe(topic, subscriber)
}

func (m *Manager) Subscribers(topic []byte, qos byte, subList *[]interface{}, qosList *[]byte) error {
	return m.ttp.Subscribers(topic, qos, subList, qosList)
}

func (m *Manager) Retain(message *packets.PublishPacket) error {
	return m.ttp.Retain(message)
}

func (m *Manager) Retained(topic []byte, messages *[]*packets.PublishPacket) error {
	return m.ttp.Retained(topic, messages)
}

func (m *Manager) Close() error {
	return m.ttp.Close()
}
