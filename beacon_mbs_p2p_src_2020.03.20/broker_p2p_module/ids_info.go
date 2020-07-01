package broker_p2p_module

import (
	"context"
	"encoding/json"
	"sync/atomic"

	mqtt "awesomeProject/beacon/mqtt_network/broker_core_module"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

var (
	nodeIdsInfoFailedOverP2P           = uint32(0)
	nodeIdsInfoFailedOverP2PGrandTotal = uint32(0)
)

//opCode (NodeIDInfoOpCode) : the information about brokerID & p2pID.

type NodeIdsInfo struct {
	BrokerID   string `json:"broker_id"`
	NodeIDAddr string `json:"node_id_address"`
}

func (n *NodeIdsInfo) Marshal() ([]byte, error) {
	return json.Marshal(n)
}

func UnmarshalNodeIdInfo(buf []byte) (*NodeIdsInfo, error) {
	info := &NodeIdsInfo{}
	err := json.Unmarshal(buf, info)

	return info, err
}

func NewNodeIdsInfoToMessageOverP2P(broker *mqtt.Broker) (*MessageOverP2P, error) {
	info := NodeIdsInfo{BrokerID: broker.BrokerID().String(), NodeIDAddr: broker.NodeID().Address}
	infoData, err := info.Marshal()
	if err != nil {
		atomic.AddUint32(&nodeIdsInfoFailedOverP2P, 1)
		return nil, err
	}
	return &MessageOverP2P{opCode: NodeIDSInfoOpCode, payLoad: infoData}, nil
}

func swapNodeIdsInfoWithPeerNode(broker *mqtt.Broker, targetIDAddress string) {
	msgOverP2P, err := NewNodeIdsInfoToMessageOverP2P(broker)
	if err != nil {
		return
	}
	res, errR := broker.Node().RequestMessage(context.TODO(), targetIDAddress, *msgOverP2P)
	if errR != nil {
		return
	}
	msg, ok := res.(MessageOverP2P)
	if !ok {
		atomic.AddUint32(&receivedMessageFailedOverP2P, 1)
		return
	}

	if !msg.IsValid() || len(msg.payLoad) == 0 {
		atomic.AddUint32(&receivedMessageFailedOverP2P, 1)
		return
	}

	ReceivedMessageOverP2PChan <- &msg
	atomic.AddUint32(&receivedMessageByteOverP2P, uint32(msg.Size()))
	atomic.AddUint32(&receivedMessageOverP2P, 1)

	broker.Node().Logger().Debug("Swap Node_Ids_Info With Peer Node : [Response] => ",
		zap.String("OpCode", msg.OpCodeString()),
		zap.String("PayLoad Size", humanize.Bytes(uint64(len(msg.payLoad)))),
	)
}

func setNodeIdsInfoMsgOverP2PForSwap(broker *mqtt.Broker) {
	msgOverP2P, err := NewNodeIdsInfoToMessageOverP2P(broker)
	if err == nil {
		nodeIdsInfoMsgOverP2PForSwap = msgOverP2P
	}
}
