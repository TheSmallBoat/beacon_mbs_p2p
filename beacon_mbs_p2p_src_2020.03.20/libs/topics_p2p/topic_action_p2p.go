package topics_p2p

import "encoding/json"

const (
	Subscribe4P2POperateCode    = byte(1)
	UnSubscribe4P2POperateCode  = byte(2)
	Brokers4P2PCheckOperateCode = byte(4)
)

type SimpleTopicAction struct {
	Operate uint8       `json:"operate"`
	Topic   []byte      `json:"topic"`
	Broker  interface{} `json:"broker"`
}

func (s *SimpleTopicAction) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func UnmarshalToSimpleTopicAction(data []byte, sTopicAction *SimpleTopicAction) error {
	return json.Unmarshal(data, sTopicAction)
}

type ActionElement struct {
	Operate byte   `json:"operate"`
	Topic   []byte `json:"topic"`
}

type TopicActions struct {
	SourceBroker interface{}     `json:"source_broker"`
	SourceNode   interface{}     `json:"source_node"`
	ActionList   []ActionElement `json:"action_list"`
}

func (t *TopicActions) Marshal() ([]byte, error) {
	return json.Marshal(t)
}

func UnmarshalTopicActions(data []byte) (*TopicActions, error) {
	tas := &TopicActions{}
	err := json.Unmarshal(data, tas)
	return tas, err
}
