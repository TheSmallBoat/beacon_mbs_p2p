package sessions

import (
	"fmt"
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	// Queue size for the ack queue
	defaultQueueSize = 16
)

type Session struct {
	// connectMessage is the CONNECT message
	connectMessage *packets.ConnectPacket

	// Will message to publish if connect is closed unexpectedly
	Will *packets.PublishPacket

	// Retained publish message
	Retained *packets.PublishPacket

	// topics stores all the topics for this session/client
	topics map[string]byte

	initialized bool

	// Serialize access to this session
	mu sync.Mutex

	id string
}

func (s *Session) Initialize(msg *packets.ConnectPacket) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.initialized {
		return fmt.Errorf("session/Init: Session already initialized")
	}

	s.connectMessage = msg
	if s.connectMessage.WillFlag {
		s.Will = packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		s.Will.Qos = s.connectMessage.Qos
		s.Will.TopicName = s.connectMessage.WillTopic
		s.Will.Payload = s.connectMessage.WillMessage
		s.Will.Retain = s.connectMessage.WillRetain
	}

	s.topics = make(map[string]byte, 1)
	s.id = msg.ClientIdentifier
	s.initialized = true

	return nil
}

func (s *Session) Update(msg *packets.ConnectPacket) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connectMessage = msg
	return nil
}

func (s *Session) RetainMessage(msg *packets.PublishPacket) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Retained = msg

	return nil
}

func (s *Session) AddTopic(topic string, qos byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return fmt.Errorf("Session/AddTopic: Session not yet initialized")
	}

	s.topics[topic] = qos

	return nil
}

func (s *Session) RemoveTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return fmt.Errorf("session/RemoveTopic: Session not yet initialized")
	}

	delete(s.topics, topic)

	return nil
}

func (s *Session) Topics() ([]string, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		return nil, nil, fmt.Errorf("session/Topics: Session not yet initialized")
	}

	var (
		topicList []string
		qosList   []byte
	)

	for k, v := range s.topics {
		topicList = append(topicList, k)
		qosList = append(qosList, v)
	}

	return topicList, qosList, nil
}

func (s *Session) ID() string {
	return s.connectMessage.ClientIdentifier
}

func (s *Session) WillFlag() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.connectMessage.WillFlag
}

func (s *Session) SetWillFlag(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.connectMessage.WillFlag = v
}

func (s *Session) CleanSession() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.connectMessage.CleanSession
}
