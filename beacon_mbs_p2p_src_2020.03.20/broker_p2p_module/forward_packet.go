package broker_p2p_module

import (
	"encoding/json"
	"errors"

	"sync/atomic"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

var (
	forwardPacketsFailedOverP2P           = uint32(0)
	forwardPacketsFailedOverP2PGrandTotal = uint32(0)
)

//opCode (PacketsOpCode) : the packets for the target broker.

type ForwardPackets struct {
	TargetBrokerId string                  `json:"target_broker_id"`
	PacketList     []packets.PublishPacket `json:"packet_list"`
}

func (f *ForwardPackets) Marshal() ([]byte, error) {
	return json.Marshal(f)
}

func UnmarshalForwardPackets(data []byte) (*ForwardPackets, error) {
	fps := &ForwardPackets{}
	err := json.Unmarshal(data, fps)

	return fps, err
}

func NewForwardPacketsToMessageOverP2P(targetBrokerIdStr string, packetList []packets.PublishPacket) (*MessageOverP2P, error) {
	if len(targetBrokerIdStr) < 1 {
		return nil, errors.New("NewForwardPacketsToMessageOverP2P => no target broker id found ")
	}
	if len(packetList) < 1 {
		return nil, errors.New("NewForwardPacketsToMessageOverP2P => no publish packet found ")
	}
	fps := ForwardPackets{TargetBrokerId: targetBrokerIdStr, PacketList: packetList}
	fpsData, err := fps.Marshal()
	if err != nil {
		atomic.AddUint32(&forwardPacketsFailedOverP2P, 1)
		return nil, err
	}
	return &MessageOverP2P{opCode: PacketsOpCode, payLoad: fpsData}, nil
}

func deliverForwardPacketsToTargetNode(targetIDAddr string, targetBrokerIdStr string, packetList []packets.PublishPacket) {
	if len(targetIDAddr) < 1 || len(targetBrokerIdStr) < 1 || len(packetList) < 1 {
		return
	}
	msgOverP2P, err := NewForwardPacketsToMessageOverP2P(targetBrokerIdStr, packetList)
	if err != nil {
		return
	}
	msgParcel := NewPendingMessageParcel(targetIDAddr, msgOverP2P)
	if msgParcel != nil {
		msgParcel.Pending()
	}
}
