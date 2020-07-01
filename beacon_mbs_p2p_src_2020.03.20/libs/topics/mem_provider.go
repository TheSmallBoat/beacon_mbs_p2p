package topics

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	QosAtMostOnce byte = iota
	QosAtLeastOnce
	QosExactlyOnce
	QosFailure = 0x80
)

var _ TheTopicsProvider = (*memProvider)(nil)

type memProvider struct {
	// Sub/unsub mutex
	smu sync.RWMutex
	// Subscription tree
	subscribeRoot *subscribeNode

	// Retained message mutex
	rmu sync.RWMutex
	// Retained messages topic tree
	retainedRoot *retainNode
}

func RegisterMemTopicsProvider() {
	Register("mem", NewMemProvider())
}

func UnRegisterMemTopicsProvider() {
	Unregister("mem")
}

// NewMemProvider returns an new instance of the memTopics, which is implements the
// TopicsProvider interface. memProvider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persistend so
// when the server goes, everything will be gone. Use with care.
func NewMemProvider() *memProvider {
	return &memProvider{
		subscribeRoot: newSubscribeNode(),
		retainedRoot:  newRetainNode(),
	}
}

func ValidQos(qos byte) bool {
	return qos == QosAtMostOnce || qos == QosAtLeastOnce || qos == QosExactlyOnce
}

func (m *memProvider) Subscribe(topic []byte, qos byte, sub interface{}) (byte, error) {
	if !ValidQos(qos) {
		return QosFailure, fmt.Errorf("topics/mem_provider/Subscribe: Invalid QoS %d", qos)
	}

	if sub == nil {
		return QosFailure, fmt.Errorf("topics/mem_provider/Subscribe: Subscriber cannot be nil")
	}

	m.smu.Lock()
	defer m.smu.Unlock()

	if qos > QosExactlyOnce {
		qos = QosExactlyOnce
	}

	if err := m.subscribeRoot.subscriberInsert(topic, qos, sub); err != nil {
		return QosFailure, err
	}

	return qos, nil
}

func (m *memProvider) Unsubscribe(topic []byte, sub interface{}) error {
	m.smu.Lock()
	defer m.smu.Unlock()

	return m.subscribeRoot.subscriberRemove(topic, sub)
}

// Returned values will be invalidated by the next Subscribers call
func (m *memProvider) Subscribers(topic []byte, qos byte, subList *[]interface{}, qosList *[]byte) error {
	if !ValidQos(qos) {
		return fmt.Errorf("topics/mem_provide/Subscribers: Invalid QoS %d", qos)
	}

	m.smu.RLock()
	defer m.smu.RUnlock()

	*subList = (*subList)[0:0]
	*qosList = (*qosList)[0:0]

	return m.subscribeRoot.subscriberMatch(topic, qos, subList, qosList)
}

func (m *memProvider) Retain(message *packets.PublishPacket) error {
	m.rmu.Lock()
	defer m.rmu.Unlock()

	// So apparently, at least according to the MQTT Conformance/Interoperability
	// Testing, that a payload of 0 means delete the retain message.
	// https://eclipse.org/paho/clients/testing/
	if len(message.Payload) == 0 {
		return m.retainedRoot.retainRemove([]byte(message.TopicName))
	}

	return m.retainedRoot.retainInsert([]byte(message.TopicName), message)
}

func (m *memProvider) Retained(topic []byte, messages *[]*packets.PublishPacket) error {
	m.rmu.RLock()
	defer m.rmu.RUnlock()

	return m.retainedRoot.retainMatch(topic, messages)
}

func (m *memProvider) Close() error {
	m.subscribeRoot = nil
	m.retainedRoot = nil
	return nil
}

// subscription nodes
type subscribeNode struct {
	// If this is the end of the topic string, then add subscribers here
	subList []interface{}
	qosList []byte

	// Otherwise add the next topic level here
	subscribeNodesMap map[string]*subscribeNode
}

func newSubscribeNode() *subscribeNode {
	return &subscribeNode{
		subscribeNodesMap: make(map[string]*subscribeNode),
	}
}

func (s *subscribeNode) subscriberInsert(topic []byte, qos byte, sub interface{}) error {
	// If there's no more topic levels, that means we are at the matching subscribeNode
	// to insert the subscriber. So let's see if there's such subscriber,
	// if so, update it. Otherwise insert it.
	if len(topic) == 0 {
		// Let's see if the subscriber is already on the list. If yes, update
		// QoS and then return.
		for i := range s.subList {
			if equal(s.subList[i], sub) {
				s.qosList[i] = qos
				return nil
			}
		}

		// Otherwise add.
		s.subList = append(s.subList, sub)
		s.qosList = append(s.qosList, qos)

		return nil
	}

	// Not the last level, so let's find or create the next level subscribeNode, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add subscribeNode if it doesn't already exist
	n, ok := s.subscribeNodesMap[level]
	if !ok {
		n = newSubscribeNode()
		s.subscribeNodesMap[level] = n
	}

	return n.subscriberInsert(rem, qos, sub)
}

// This remove implementation ignores the QoS, as long as the subscriber
// matches then it's removed
func (s *subscribeNode) subscriberRemove(topic []byte, sub interface{}) error {
	// If the topic is empty, it means we are at the final matching subscribeNode. If so,
	// let's find the matching subscribers and remove them.
	if len(topic) == 0 {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		if sub == nil {
			s.subList = s.subList[0:0]
			s.qosList = s.qosList[0:0]
			return nil
		}

		// If we find the subscriber then remove it from the list. Technically
		// we just overwrite the slot by shifting all other items up by one.
		for i := range s.subList {
			if equal(s.subList[i], sub) {
				s.subList = append(s.subList[:i], s.subList[i+1:]...)
				s.qosList = append(s.qosList[:i], s.qosList[i+1:]...)
				return nil
			}
		}

		return fmt.Errorf("topics/mem_provider/subscriberRemove: No topic found for subscriber")
	}

	// Not the last level, so let's find the next level subscribeNode, and recursively
	// call it's remove().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the subscribeNode that matches the topic level
	n, ok := s.subscribeNodesMap[level]
	if !ok {
		return fmt.Errorf("topics/mem_provider/subscriberRemove: No topic found")
	}

	// Remove the subscriber from the next level subscribeNode
	if err := n.subscriberRemove(rem, sub); err != nil {
		return err
	}

	// If there are no more subscribers and subscribeNode to the next level we just visited
	// let's remove it
	if len(n.subList) == 0 && len(n.subscribeNodesMap) == 0 {
		delete(s.subscribeNodesMap, level)
	}

	return nil
}

// subscriberMatch() returns all the subscribers that are subscribed to the topic. Given a topic
// with no wildcards (publish topic), it returns a list of subscribers that subscribes
// to the topic. For each of the level names, it's a match
// - if there are subscribers to '#', then all the subscribers are added to result set
func (s *subscribeNode) subscriberMatch(topic []byte, qos byte, subList *[]interface{}, qosList *[]byte) error {
	// If the topic is empty, it means we are at the final matching subscribeNode. If so,
	// let's find the subscribers that match the qos and append them to the list.
	if len(topic) == 0 {
		s.matchQos(qos, subList, qosList)
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	for k, n := range s.subscribeNodesMap {
		// If the key is "#", then these subscribers are added to the result set
		if k == MWC {
			n.matchQos(qos, subList, qosList)
		} else if k == SWC || k == level {
			if err := n.subscriberMatch(rem, qos, subList, qosList); err != nil {
				return err
			}
		}
	}

	return nil
}

// retained message nodes
type retainNode struct {
	// If this is the end of the topic string, then add retained messages here
	message *packets.PublishPacket
	// Otherwise add the next topic level here
	retainNodesMap map[string]*retainNode
}

func newRetainNode() *retainNode {
	return &retainNode{
		retainNodesMap: make(map[string]*retainNode),
	}
}

func (r *retainNode) retainInsert(topic []byte, message *packets.PublishPacket) error {
	// If there's no more topic levels, that means we are at the matching retainNode.
	if len(topic) == 0 {
		// Reuse the message if possible
		if r.message == nil {
			r.message = message
		}

		return nil
	}

	// Not the last level, so let's find or create the next level subscribeNode, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add subscribeNode if it doesn't already exist
	n, ok := r.retainNodesMap[level]
	if !ok {
		n = newRetainNode()
		r.retainNodesMap[level] = n
	}

	return n.retainInsert(rem, message)
}

// Remove the retained message for the supplied topic
func (r *retainNode) retainRemove(topic []byte) error {
	// If the topic is empty, it means we are at the final matching retainNode. If so,
	// let's remove the buffer and message.
	if len(topic) == 0 {
		r.message = nil
		return nil
	}

	// Not the last level, so let's find the next level retainNode, and recursively
	// call it's remove().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the retainNode that matches the topic level
	n, ok := r.retainNodesMap[level]
	if !ok {
		return fmt.Errorf("topics/mem_provider/retainRemove: No topic found")
	}

	// Remove the subscriber from the next level retainNode
	if err := n.retainRemove(rem); err != nil {
		return err
	}

	// If there are no more retainNode to the next level we just visited let's remove it
	if len(n.retainNodesMap) == 0 {
		delete(r.retainNodesMap, level)
	}

	return nil
}

// retainMatch() finds the retained messages for the topic and qos provided. It's somewhat
// of a reverse match compare to match() since the supplied topic can contain
// wildcards, whereas the retained message topic is a full (no wildcard) topic.
func (r *retainNode) retainMatch(topic []byte, messageList *[]*packets.PublishPacket) error {
	// If the topic is empty, it means we are at the final matching retainNode. If so,
	// add the retained msg to the list.
	if len(topic) == 0 {
		if r.message != nil {
			*messageList = append(*messageList, r.message)
		}
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	if level == MWC {
		// If '#', add all retained messages starting this node
		r.allRetained(messageList)
	} else if level == SWC {
		// If '+', check all nodes at this level. Next levels must be matched.
		for _, n := range r.retainNodesMap {
			if err := n.retainMatch(rem, messageList); err != nil {
				return err
			}
		}
	} else {
		// Otherwise, find the matching node, go to the next level
		if n, ok := r.retainNodesMap[level]; ok {
			if err := n.retainMatch(rem, messageList); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *retainNode) allRetained(messageList *[]*packets.PublishPacket) {
	if r.message != nil {
		*messageList = append(*messageList, r.message)
	}

	for _, n := range r.retainNodesMap {
		n.allRetained(messageList)
	}
}

const (
	stateCHR byte = iota // Regular character
	stateMWC             // Multi-level wildcard
	stateSWC             // Single-level wildcard
	//stateSEP             // Topic level separator
	stateSYS // System level topic ($)
)

func NextTopicLevel(topic []byte) ([]byte, []byte, error) {
	return nextTopicLevel(topic)
}

// Returns topic level, remaining topic levels and any errors
func nextTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := stateCHR

	for i, c := range topic {
		switch c {
		case '/':
			if s == stateMWC {
				return nil, nil, fmt.Errorf("topics/mem_provider/nextTopicLevel: Multi-level wildcard found in topic and it's not at the last level")
			}

			if i == 0 {
				return []byte(SWC), topic[i+1:], nil
			}

			return topic[:i], topic[i+1:], nil

		case '#':
			if i != 0 {
				return nil, nil, fmt.Errorf("topics/mem_provider/nextTopicLevel: Wildcard character '#' must occupy entire topic level")
			}

			s = stateMWC

		case '+':
			if i != 0 {
				return nil, nil, fmt.Errorf("topics/mem_provider/nextTopicLevel: Wildcard character '+' must occupy entire topic level")
			}

			s = stateSWC

		case '$':
			/*if i == 0 {
				return nil, nil, fmt.Errorf("topics/mem_provider/nextTopicLevel: Cannot publish to topics containing '$' character")
			}*/

			s = stateSYS

		default:
			if s == stateMWC || s == stateSWC {
				return nil, nil, fmt.Errorf("topics/mem_provider/nextTopicLevel: Wildcard characters '#' and '+' must occupy entire topic level")
			}

			s = stateCHR
		}
	}

	// If we got here that means we didn't hit the separator along the way, so the
	// topic is either empty, or does not contain a separator. Either way, we return
	// the full topic
	return topic, nil, nil
}

// The QoS of the payload messages sent in response to a subscription must be the
// minimum of the QoS of the originally published message (in this case, it's the
// qos parameter) and the maximum QoS granted by the server (in this case, it's
// the QoS in the topic tree).
//
// It's also possible that even if the topic matches, the subscriber is not included
// due to the QoS granted is lower than the published message QoS. For example,
// if the client is granted only QoS 0, and the publish message is QoS 1, then this
// client is not to be send the published message.
func (s *subscribeNode) matchQos(qos byte, subList *[]interface{}, qosList *[]byte) {
	for _, sub := range s.subList {
		// If the published QoS is higher than the subscriber QoS, then we skip the
		// subscriber. Otherwise, add to the list.
		// if qos >= this.qos[i] {
		*subList = append(*subList, sub)
		*qosList = append(*qosList, qos)
		// }
	}
}

func Equal(k1, k2 interface{}) bool {
	return equal(k1, k2)
}

func equal(k1, k2 interface{}) bool {
	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
		return false
	}

	if reflect.ValueOf(k1).Kind() == reflect.Func {
		return &k1 == &k2
	}

	if k1 == k2 {
		return true
	}

	switch k1 := k1.(type) {
	case string:
		return k1 == k2.(string)

	case int64:
		return k1 == k2.(int64)

	case int32:
		return k1 == k2.(int32)

	case int16:
		return k1 == k2.(int16)

	case int8:
		return k1 == k2.(int8)

	case int:
		return k1 == k2.(int)

	case float32:
		return k1 == k2.(float32)

	case float64:
		return k1 == k2.(float64)

	case uint:
		return k1 == k2.(uint)

	case uint8:
		return k1 == k2.(uint8)

	case uint16:
		return k1 == k2.(uint16)

	case uint32:
		return k1 == k2.(uint32)

	case uint64:
		return k1 == k2.(uint64)

	case uintptr:
		return k1 == k2.(uintptr)
	}

	return false
}
