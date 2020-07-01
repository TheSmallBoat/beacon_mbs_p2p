package topics_p2p

import (
	"fmt"
	"sync"

	"awesomeProject/beacon/mqtt_network/libs/topics"
)

var _ TheTopicsProvider4P2P = (*memProvider4P2P)(nil)

type memProvider4P2P struct {
	// Sub/unsub mutex
	smu4P2P sync.RWMutex
	// Subscription tree
	subscribeRoot4P2P *subscribeNode4P2P
}

func RegisterMemTopicsProvider4P2P() {
	Register4P2P("mem", NewMemProvider4P2P())
}

func UnRegisterMemTopicsProvider4P2P() {
	Unregister4P2P("mem")
}

// NewMemProvider returns an new instance of the memTopics, which is implements the
// TopicsProvider interface. memProvider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persisted so
// when the server goes, everything will be gone. Use with care.
func NewMemProvider4P2P() *memProvider4P2P {
	return &memProvider4P2P{
		subscribeRoot4P2P: newSubscribeNode4P2P(),
	}
}

func (m *memProvider4P2P) Subscribe4P2P(topic []byte, broker interface{}) error {
	if broker == nil {
		return fmt.Errorf("topics_p2p/mem_provider_p2p/Subscribe4P2P: broker cannot be nil")
	}

	m.smu4P2P.Lock()
	defer m.smu4P2P.Unlock()

	if err := m.subscribeRoot4P2P.subscriber4P2PInsert(topic, broker); err != nil {
		return err
	}

	return nil
}

func (m *memProvider4P2P) Unsubscribe4P2P(topic []byte, broker interface{}) error {
	m.smu4P2P.Lock()
	defer m.smu4P2P.Unlock()

	return m.subscribeRoot4P2P.subscriber4P2PRemove(topic, broker)
}

// Returned values will be invalidated by the next Brokers call
func (m *memProvider4P2P) Brokers4P2P(topic []byte, brokerList *[]interface{}) error {
	m.smu4P2P.RLock()
	defer m.smu4P2P.RUnlock()

	*brokerList = (*brokerList)[0:0]

	return m.subscribeRoot4P2P.subscriber4P2PMatch(topic, brokerList)
}

func (m *memProvider4P2P) BrokerTopics4P2PSimpleAction(action SimpleTopicAction) error {
	return m.brokerTopics4P2PSimpleAction(action)
}

func (m *memProvider4P2P) BrokerTopics4P2PSimpleActionToJSON(action SimpleTopicAction) ([]byte, error) {
	err := m.brokerTopics4P2PSimpleAction(action)
	if err == nil {
		return action.Marshal()
	}
	return nil, err
}

func (m *memProvider4P2P) BrokerTopics4P2PSimpleActionFromJSON(data []byte) error {
	action := SimpleTopicAction{}
	err := UnmarshalToSimpleTopicAction(data, &action)
	if err == nil {
		err = m.brokerTopics4P2PSimpleAction(action)
	}
	return err
}

// Let's see if the broker is already on the list.
func checkExist(brokerList []interface{}, broker interface{}) bool {
	for i := range brokerList {
		if topics.Equal(brokerList[i], broker) {
			return true
		}
	}
	return false
}

func (m *memProvider4P2P) brokerTopics4P2PSimpleAction(topicAction SimpleTopicAction) error {
	var err error

	switch topicAction.Operate {
	case Subscribe4P2POperateCode:
		err = m.Subscribe4P2P(topicAction.Topic, topicAction.Broker)
	case UnSubscribe4P2POperateCode:
		err = m.Unsubscribe4P2P(topicAction.Topic, topicAction.Broker)
	case Brokers4P2PCheckOperateCode:
		brokerList := make([]interface{}, 32)
		err = m.Brokers4P2P(topicAction.Topic, &brokerList)
		if err == nil {
			if !checkExist(brokerList, topicAction.Broker) {
				err = fmt.Errorf("topics_p2p/mem_provider_p2p/brokerTopics4P2PAction: Broker Not Found ")
			}
		}
	default:
		err = fmt.Errorf("topics_p2p/mem_provider_p2p/brokerTopics4P2PAction: Unkown Operate Code ")
	}

	return err
}

func (m *memProvider4P2P) BrokerTopics4P2PActions(actions TopicActions) error {
	return m.brokerTopics4P2PActions(actions)
}

func (m *memProvider4P2P) brokerTopics4P2PActions(actions TopicActions) error {
	var err error
	errNum := 0
	broker := actions.SourceBroker
	actionList := actions.ActionList

	for i, action := range actionList {
		switch action.Operate {
		case Subscribe4P2POperateCode:
			err = m.Subscribe4P2P(action.Topic, broker)
		case UnSubscribe4P2POperateCode:
			err = m.Unsubscribe4P2P(action.Topic, broker)
		case Brokers4P2PCheckOperateCode:
			brokerList := make([]interface{}, 32)
			err = m.Brokers4P2P(action.Topic, &brokerList)
			if err == nil {
				if !checkExist(brokerList, broker) {
					err = fmt.Errorf("topics_p2p/mem_provider_p2p/BrokerTopics4P2PActions: Broker [%v] Not Found => No.[%d], topic: [%v], operate code: [%v]", broker, i, action.Topic, action.Operate)
				}
			}
		default:
			err = fmt.Errorf("topics_p2p/mem_provider_p2p/BrokerTopics4P2PActions: Unkown Operate Code => broker: [%v], No.[%d], topic: [%v],operate code: [%v]", broker, i, action.Topic, action.Operate)
		}
		if err != nil {
			errNum++
		}
	}
	if errNum > 0 {
		err = fmt.Errorf("topics_p2p/mem_provider_p2p/BrokerTopics4P2PActions: have [%d] errors => broker: [%v]", errNum, broker)
	} else {
		err = nil
	}
	return err
}

func (m *memProvider4P2P) BrokerTopics4P2PActionsToJSON(actions TopicActions) ([]byte, error) {
	err := m.brokerTopics4P2PActions(actions)
	if err == nil {
		return actions.Marshal()
	}
	return nil, err
}

func (m *memProvider4P2P) BrokerTopics4P2PActionsFromJSON(data []byte) error {
	//actions := &TopicActions{}
	//err := UnmarshalToTopicActions(data, actions)
	actions, err := UnmarshalTopicActions(data)
	if err == nil {
		err = m.brokerTopics4P2PActions(*actions)
	}
	return err
}

func (m *memProvider4P2P) Close() error {
	m.subscribeRoot4P2P = nil
	return nil
}

// subscription nodes for p2p-network
type subscribeNode4P2P struct {
	// If this is the end of the topic string, then add brokers here
	brokerList []interface{}

	// Otherwise add the next topic level here
	subscribeNodes4P2PMap map[string]*subscribeNode4P2P
}

func newSubscribeNode4P2P() *subscribeNode4P2P {
	return &subscribeNode4P2P{
		subscribeNodes4P2PMap: make(map[string]*subscribeNode4P2P),
	}
}

func (s *subscribeNode4P2P) subscriber4P2PInsert(topic []byte, broker interface{}) error {
	// If there's no more topic levels, that means we are at the matching subscribeNode4P2P
	// to insert the subscriber. So let's see if there's such broker,
	// if so, update it. Otherwise insert it.
	if len(topic) == 0 {
		// Let's see if the broker is already on the list. If yes return.
		for i := range s.brokerList {
			if topics.Equal(s.brokerList[i], broker) {
				return nil
			}
		}

		// Otherwise add.
		s.brokerList = append(s.brokerList, broker)

		return nil
	}

	// Not the last level, so let's find or create the next level subscribeNode4P2P, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := topics.NextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add subscribeNode4P2P if it doesn't already exist
	n, ok := s.subscribeNodes4P2PMap[level]
	if !ok {
		n = newSubscribeNode4P2P()
		s.subscribeNodes4P2PMap[level] = n
	}

	return n.subscriber4P2PInsert(rem, broker)
}

// This remove implementation as long as the broker matches then it's removed
func (s *subscribeNode4P2P) subscriber4P2PRemove(topic []byte, broker interface{}) error {
	// If the topic is empty, it means we are at the final matching subscribeNode4P2P. If so,
	// let's find the matching brokers and remove them.
	if len(topic) == 0 {
		// If broker == nil, then it's signal to remove ALL brokers
		if broker == nil {
			s.brokerList = s.brokerList[0:0]
			return nil
		}

		// If we find the brokers then remove it from the list. Technically
		// we just overwrite the slot by shifting all other items up by one.
		for i := range s.brokerList {
			if topics.Equal(s.brokerList[i], broker) {
				s.brokerList = append(s.brokerList[:i], s.brokerList[i+1:]...)
				return nil
			}
		}

		return fmt.Errorf("topics_p2p/mem_provider_p2p/subscriber4P2PRemove: No topic found for broker")
	}

	// Not the last level, so let's find the next level subscribeNode4P2P, and recursively
	// call it's remove().

	// ntl = next topic level
	ntl, rem, err := topics.NextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the subscribeNode4P2P that matches the topic level
	n, ok := s.subscribeNodes4P2PMap[level]
	if !ok {
		return fmt.Errorf("topics_p2p/mem_provider_p2p/subscriber4P2PRemove: No topic found")
	}

	// Remove the broker from the next level subscribeNode4P2P
	if err := n.subscriber4P2PRemove(rem, broker); err != nil {
		return err
	}

	// If there are no more brokers and subscribeNode4P2P to the next level we just visited
	// let's remove it
	if len(n.brokerList) == 0 && len(n.subscribeNodes4P2PMap) == 0 {
		delete(s.subscribeNodes4P2PMap, level)
	}

	return nil
}

// subscriber4P2PMatch() returns all the brokers that are subscribed to the topic. Given a topic
// with no wildcards (publish topic), it returns a list of brokers that subscribes
// to the topic. For each of the level names, it's a match
// - if there are brokers to '#', nothing to do.
func (s *subscribeNode4P2P) subscriber4P2PMatch(topic []byte, brokerList *[]interface{}) error {
	// If the topic is empty, it means we are at the final matching subscribeNode4P2P. If so,
	// let's find the brokers that match and append them to the list.
	if len(topic) == 0 {
		for _, broker := range s.brokerList {
			*brokerList = append(*brokerList, broker)
		}
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := topics.NextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	for k, n := range s.subscribeNodes4P2PMap {
		//If the key is "#", then these brokers are added to the result set
		if k == topics.MWC {
			for _, broker := range n.brokerList {
				*brokerList = append(*brokerList, broker)
			}
			return nil
		} else if k == topics.SWC || k == level {
			if err := n.subscriber4P2PMatch(rem, brokerList); err != nil {
				return err
			}
		}
	}

	return nil
}
