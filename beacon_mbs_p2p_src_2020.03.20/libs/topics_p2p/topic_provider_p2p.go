package topics_p2p

import (
	"fmt"
)

var (
	providers4P2P = make(map[string]TheTopicsProvider4P2P)
)

type TheTopicsProvider4P2P interface {
	Subscribe4P2P(topic []byte, broker interface{}) error
	Unsubscribe4P2P(topic []byte, broker interface{}) error
	Brokers4P2P(topic []byte, brokerList *[]interface{}) error
	BrokerTopics4P2PSimpleAction(action SimpleTopicAction) error
	BrokerTopics4P2PSimpleActionToJSON(action SimpleTopicAction) ([]byte, error)
	BrokerTopics4P2PSimpleActionFromJSON(data []byte) error
	BrokerTopics4P2PActions(actions TopicActions) error
	BrokerTopics4P2PActionsToJSON(actions TopicActions) ([]byte, error)
	BrokerTopics4P2PActionsFromJSON(data []byte) error
	Close() error
}

func Register4P2P(name string, provider TheTopicsProvider4P2P) {
	if provider == nil {
		panic("topics_p2p/topic_provider_p2p: Register provider of p2p is nil")
	}

	if _, dup := providers4P2P[name]; dup {
		panic("topics_p2p/topic_provider_p2p: Register called twice for provider of p2p " + name)
	}

	providers4P2P[name] = provider
}

func Unregister4P2P(name string) {
	delete(providers4P2P, name)
}

type Manager4P2P struct {
	ttp TheTopicsProvider4P2P
}

func NewManager4P2P(providerName string) (*Manager4P2P, error) {
	p, ok := providers4P2P[providerName]
	if !ok {
		return nil, fmt.Errorf("topics_p2p/topic_provider_p2p: unknown provider %q", providerName)
	}

	return &Manager4P2P{ttp: p}, nil
}

func (m *Manager4P2P) Subscribe4P2P(topic []byte, broker interface{}) error {
	return m.ttp.Subscribe4P2P(topic, broker)
}

func (m *Manager4P2P) Unsubscribe4P2P(topic []byte, broker interface{}) error {
	return m.ttp.Unsubscribe4P2P(topic, broker)
}

func (m *Manager4P2P) Brokers4P2P(topic []byte, brokerList *[]interface{}) error {
	return m.ttp.Brokers4P2P(topic, brokerList)
}

func (m *Manager4P2P) BrokerTopics4P2PSimpleAction(action SimpleTopicAction) error {
	return m.ttp.BrokerTopics4P2PSimpleAction(action)
}

// Execute operation and export to data with json format
func (m *Manager4P2P) BrokerTopics4P2PSimpleActionToJSON(action SimpleTopicAction) ([]byte, error) {
	return m.ttp.BrokerTopics4P2PSimpleActionToJSON(action)
}

// Importing data with json format and execute it's operation
func (m *Manager4P2P) BrokerTopics4P2PSimpleActionFromJSON(data []byte) error {
	return m.ttp.BrokerTopics4P2PSimpleActionFromJSON(data)
}

func (m *Manager4P2P) BrokerTopics4P2PActions(actions TopicActions) error {
	return m.ttp.BrokerTopics4P2PActions(actions)
}

// Execute the operations and export to data with json format
func (m *Manager4P2P) BrokerTopics4P2PActionsToJSON(actions TopicActions) ([]byte, error) {
	return m.ttp.BrokerTopics4P2PActionsToJSON(actions)
}

// Importing data with json format and execute their operations
func (m *Manager4P2P) BrokerTopics4P2PActionsFromJSON(data []byte) error {
	return m.ttp.BrokerTopics4P2PActionsFromJSON(data)
}

func (m *Manager4P2P) Close() error {
	return m.ttp.Close()
}
