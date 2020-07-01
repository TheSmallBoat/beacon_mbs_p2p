package broker_p2p_module

import (
	"errors"
	"go.uber.org/zap"
	"sync/atomic"

	mqtt "awesomeProject/beacon/mqtt_network/broker_core_module"
	"awesomeProject/beacon/mqtt_network/libs/topics_p2p"
)

var (
	topicActionsFailedOverP2P           = uint32(0)
	topicActionsFailedOverP2PGrandTotal = uint32(0)
)

// TopicActions add a attribute for the SourceNode, also send the NodeIdsInfo together.

// opCode (ActionsOpCode) : the actions will be run in other nodes.

func NewTopicActionsToMessageOverP2P(sourceBrokerIdStr string, sourceNodeIdAddr string, aeList []topics_p2p.ActionElement) (*MessageOverP2P, error) {
	if len(aeList) < 1 {
		return nil, errors.New("NewTopicActionsToMessageOverP2P => no any action element found ")
	}
	actions := topics_p2p.TopicActions{
		SourceBroker: sourceBrokerIdStr,
		SourceNode:   sourceNodeIdAddr,
		ActionList:   aeList,
	}
	actionsData, err := actions.Marshal()
	if err != nil {
		atomic.AddUint32(&topicActionsFailedOverP2P, 1)
		return nil, err
	}
	return &MessageOverP2P{opCode: TopicActionsOpCode, payLoad: actionsData}, nil
}

func deliverTopicActionsToTargetNode(sourceBrokerIdStr string, sourceNodeIdAddr string, targetNodeIDAddr string, aeList []topics_p2p.ActionElement) {
	if len(aeList) < 1 {
		return
	}
	msgOverP2P, err := NewTopicActionsToMessageOverP2P(sourceBrokerIdStr, sourceNodeIdAddr, aeList)
	if err == nil {
		msgParcel := NewPendingMessageParcel(targetNodeIDAddr, msgOverP2P)
		msgParcel.Pending()
	}
}

// Checking all topics for the broker, when find a new peer node, need to send all existed topics at once.
func processExistedTopicsAndDeliverToTargetNodeAtOnce(broker *mqtt.Broker, targetNodeIDAddr string) {
	aeList := broker.ExistedTopicActions()
	if len(aeList) < 1 {
		return
	}
	deliverTopicActionsToTargetNode(broker.BrokerID().String(), broker.NodeID().Address, targetNodeIDAddr, aeList)
	broker.Node().Logger().Debug("Existed Topics And Deliver To Target Node At Once ",
		zap.String("broker_id", broker.BrokerID().String()),
		zap.String("node_id_address", broker.NodeID().Address),
	)
}

func processExistedTopicsAndDeliverToPeerNodes(broker *mqtt.Broker) {
	aeList := broker.ExistedTopicActions()
	if len(aeList) < 1 {
		return
	}
	deliverTopicActionsToPeerNodes(broker, aeList)
}

// Deliver TopicActions to each peer-nodes
func deliverTopicActionsToPeerNodes(broker *mqtt.Broker, aeList []topics_p2p.ActionElement) {
	if len(aeList) < 1 {
		return
	}
	for _, tid := range broker.Overlay().Table().Peers() {
		deliverTopicActionsToTargetNode(broker.BrokerID().String(), broker.NodeID().Address, tid.Address, aeList)
		broker.Node().Logger().Debug("Deliver TopicActions To Peer Nodes ",
			zap.String("broker_id", broker.BrokerID().String()),
			zap.String("node_id_address", broker.NodeID().Address),
		)
	}
}
