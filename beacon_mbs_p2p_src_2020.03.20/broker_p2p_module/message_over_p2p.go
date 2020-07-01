package broker_p2p_module

import (
	"context"
	"errors"
	"fmt"
	"time"

	mqtt "awesomeProject/beacon/mqtt_network/broker_core_module"
	"awesomeProject/beacon/mqtt_network/libs/topics_p2p"

	p2p "awesomeProject/beacon/p2p_network/core_module"
)

const (
	MessageHeader      = byte(0x99)
	NodeIDSInfoOpCode  = byte(1)
	TopicActionsOpCode = byte(2)
	PacketsOpCode      = byte(4)
	UnknownOpCode      = byte(0x88)
)

var nodeIdsInfoMsgOverP2PForSwap *MessageOverP2P

type MessageOverP2P struct {
	header  byte
	opCode  byte
	payLoad []byte
}

// after received a NodeIdsInfo,then send
func (m *MessageOverP2P) swapNodeIdsInfo(ctx p2p.HandlerContext) bool {
	if nodeIdsInfoMsgOverP2PForSwap == nil {
		return false
	}

	if m.opCode == NodeIDSInfoOpCode && nodeIdsInfoMsgOverP2PForSwap.opCode == NodeIDSInfoOpCode {
		err := ctx.SendMessage(nodeIdsInfoMsgOverP2PForSwap)
		if err == nil {
			return true
		}
	}
	return false
}

func (m *MessageOverP2P) IsValid() bool {
	return m.header == MessageHeader
}

func (m *MessageOverP2P) Size() int {
	return 2 + len(m.payLoad)
}

func (m *MessageOverP2P) CheckOpCode() {
	if m.opCode != NodeIDSInfoOpCode && m.opCode != TopicActionsOpCode && m.opCode != PacketsOpCode && m.opCode != UnknownOpCode {
		m.opCode = UnknownOpCode
	}
}

func (m *MessageOverP2P) OpCode() byte {
	m.CheckOpCode()
	return m.opCode
}

func (m *MessageOverP2P) PayLoad() []byte {
	return m.payLoad[:]
}

func (m *MessageOverP2P) OpCodeString() string {
	var opCodeStr = ""
	switch m.opCode {
	case NodeIDSInfoOpCode:
		opCodeStr = "NodeIDInfo's OpCode"
	case TopicActionsOpCode:
		opCodeStr = "TopicActions' OpCode"
	case PacketsOpCode:
		opCodeStr = "Packets' OpCode"
	case UnknownOpCode:
		opCodeStr = "Unknown OpCode"
	default:
		m.opCode = UnknownOpCode
		opCodeStr = "Unknown OpCode"
	}
	return opCodeStr
}

func (m MessageOverP2P) Marshal() []byte {
	return m.ByteMarshal()
}

func (m *MessageOverP2P) ByteMarshal() []byte {
	buf := make([]byte, 2+len(m.payLoad))
	buf[0] = MessageHeader
	buf[1] = m.opCode
	copy(buf[2:2+len(m.payLoad)], m.payLoad)
	return buf
}

func UnmarshalMessageOverP2P(buf []byte) (MessageOverP2P, error) {
	return MessageOverP2P{header: buf[0], opCode: buf[1], payLoad: buf[2:]}, nil
}

// Send MessageOverP2P to target node.
func (m *MessageOverP2P) sendMessageOverP2PToTargetNode(node *p2p.Node, targetNodeIDAddress string) error {
	if len(targetNodeIDAddress) < 1 {
		return errors.New("SendMessageOverP2PToTargetNode => no target node id address found ")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := node.SendMessage(ctx, targetNodeIDAddress, *m)
	cancel()

	return err
}

// According OpCode to execute the related function.
func (m *MessageOverP2P) ExecuteTaskAccordingMessageOverP2P(b *mqtt.Broker) error {
	switch m.OpCode() {
	case NodeIDSInfoOpCode:
		nInf, err := UnmarshalNodeIdInfo(m.payLoad)
		if err != nil {
			return err
		}
		return b.BrokerNode().NodeIdAddrStoreToMap(nInf.BrokerID, nInf.NodeIDAddr)
	case TopicActionsOpCode:
		// Todo : The consistency of topic subscriptions needs to be maintained.
		// Single actions, such as subscribe and unsubscribe, can be performed in order.
		// When the latest full-topic-list comes, that need to compare the existing records, and then generate add and delete operation.

		tas, err := topics_p2p.UnmarshalTopicActions(m.payLoad)
		if err != nil {
			return err
		}
		sb, okb := tas.SourceBroker.(string)
		sn, okn := tas.SourceNode.(string)
		if okb && okn && len(sb) > 0 && len(sn) > 0 {
			//The source node sends the MessageOverP2P, so TopicActions will definitely contain the exact NodeIDSInfo.
			err = b.BrokerNode().NodeIdAddrStoreToMap(sb, sn)
			if err != nil {
				return err
			}
		}
		if len(tas.ActionList) > 0 {
			return b.TopicsManager4P2P().BrokerTopics4P2PActions(*tas)
		}
	case PacketsOpCode:
		fps, err := UnmarshalForwardPackets(m.payLoad)
		if err != nil {
			return err
		}
		if fps.TargetBrokerId == b.BrokerID().String() {
			if len(fps.PacketList) > 0 {
				for _, pkt := range fps.PacketList {
					b.SubmitPublishPacketsWorkTask(&pkt)
				}
			} else {
				err = fmt.Errorf("broker_p2p_module/node_message/ExecuteTaskAccordingMessageOverP2P: No packet found ... [%s] ",
					fps.TargetBrokerId,
				)
				return err
			}
		} else {
			err = fmt.Errorf("broker_p2p_module/node_message/ExecuteTaskAccordingMessageOverP2P: the target broker [%s] not match this broker %s",
				fps.TargetBrokerId,
				b.BrokerID().String(),
			)
			return err
		}
	case UnknownOpCode:
		return errors.New("core_module/broker_p2p/ExecuteTaskAccordingMessageOverP2P error : Unknown OpCode")
	default:

	}

	return nil
}
