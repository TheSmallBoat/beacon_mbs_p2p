package broker_core_module

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"awesomeProject/beacon/mqtt_network/libs/topics_p2p"

	p2p "awesomeProject/beacon/p2p_network/core_module"
	"awesomeProject/beacon/p2p_network/libs/cryptographic"
	"awesomeProject/beacon/p2p_network/libs/kademlia"

	"github.com/dustin/go-humanize"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/rs/xid"
	"go.uber.org/zap"
)

const (
	defaultActionElementChanSize           = 2
	defaultForwardPacketChanSize           = 16
	defaultCandidateForwardConfirmChanSize = 256 // just only one channel to process the candidate-forward-confirm task.

	defaultActionElementListAcceptTimeInterval  = 128 // Millisecond
	defaultTopicActionsNotificationTimeInterval = 10  // seconds

	defaultForwardPacketListAcceptTimeInterval    = 128 // Millisecond
	defaultForwardPacketsNotificationTimeInterval = 10  // seconds

	defaultTopicListCapacity         = 64
	defaultActionElementListCapacity = 32
	defaultForwardPacketListCapacity = 128
	defaultInfoListCapacity          = 64
)

type BrokerP2PNode struct {
	mu sync.Mutex

	subNumMap sync.Map
	nodeIDMap sync.Map

	brokerID *xid.ID
	nodeID   *cryptographic.ID
	overlay  *kademlia.Protocol

	actionElementChan    chan topics_p2p.ActionElement
	forwardPacketChanMap map[string]chan *packets.PublishPacket

	// After the publish-packet is processed for this broker, it will be submitted to this channel
	// and waits for the subsequent forwarding confirmation process.
	candidateForwardConfirmChan chan *packets.PublishPacket
	packetForwardMetrics        *PacketForwardMetrics

	deliverForwardPacketsToTargetNode func(string, string, []packets.PublishPacket)
	deliverTopicActionsToPeerNodes    func(*Broker, []topics_p2p.ActionElement)
}

func NewBrokerP2PNode() *BrokerP2PNode {
	id := xid.New()
	return &BrokerP2PNode{
		mu:                                sync.Mutex{},
		subNumMap:                         sync.Map{},
		nodeIDMap:                         sync.Map{},
		brokerID:                          &id,
		nodeID:                            nil,
		overlay:                           nil,
		actionElementChan:                 make(chan topics_p2p.ActionElement, defaultActionElementChanSize),
		forwardPacketChanMap:              make(map[string]chan *packets.PublishPacket),
		candidateForwardConfirmChan:       make(chan *packets.PublishPacket, defaultCandidateForwardConfirmChanSize),
		packetForwardMetrics:              &PacketForwardMetrics{},
		deliverForwardPacketsToTargetNode: nil,
		deliverTopicActionsToPeerNodes:    nil,
	}
}

func (b *BrokerP2PNode) checkNil() error {
	if b.overlay == nil {
		return errors.New("broker need set overlay pointer, cannot be nil")
	}
	if b.deliverForwardPacketsToTargetNode == nil {
		return errors.New("broker need register DeliverForwardPacketsToTargetNode, cannot be nil")
	}
	if b.deliverTopicActionsToPeerNodes == nil {
		return errors.New("broker need register DeliverTopicActionsToPeerNodes, cannot be nil")
	}
	return nil
}

func (b *BrokerP2PNode) BrokerID() xid.ID {
	return *b.brokerID
}

func (b *BrokerP2PNode) Overlay() *kademlia.Protocol {
	return b.overlay
}

func (b *BrokerP2PNode) SetOverlay(overlay *kademlia.Protocol) {
	b.overlay = overlay
}

func (b *BrokerP2PNode) SetNodeId(id *cryptographic.ID) {
	b.nodeID = id
	_ = b.NodeIdAddrStoreToMap(b.brokerID.String(), id.Address)
}

func (b *BrokerP2PNode) NodeIdAddrStoreToMap(brokerIDStr string, nodeIdAddr string) error {
	if len(brokerIDStr) < 1 || len(nodeIdAddr) < 1 || nodeIdAddr == "ERROR" {
		return errors.New("broker_id or node_id_str too short or node_id_str get 'ERROR'")
	}
	var exist bool
	var old interface{}
	old, exist = b.nodeIDMap.Load(brokerIDStr)
	if exist {
		ol, ok := old.(string)
		if ok && ol == nodeIdAddr {
			// Nothing to do.
			return nil
		}
	}
	b.nodeIDMap.Store(brokerIDStr, nodeIdAddr)
	return nil
}

func (b *BrokerP2PNode) NodeIdAddrGetFromMap(brokerIDStr string) string {
	if len(brokerIDStr) < 1 {
		return "ERROR"
	}
	var exist bool
	var old interface{}
	old, exist = b.nodeIDMap.Load(brokerIDStr)
	if exist {
		ol, ok := old.(string)
		if ok {
			return ol
		}
	}
	return "ERROR"
}

func (b *BrokerP2PNode) ProcessSubNumMapForAdd(topic string) {
	var exist bool
	var old interface{}
	old, exist = b.subNumMap.Load(topic)
	if exist {
		ol, ok := old.(int)
		if ok {
			b.subNumMap.Store(topic, ol+1)
		}
	} else {
		b.subNumMap.Store(topic, 1)
		b.actionElementChan <- topics_p2p.ActionElement{Operate: topics_p2p.Subscribe4P2POperateCode, Topic: []byte(topic)}
	}
}
func (b *BrokerP2PNode) ProcessSubNumMapForDel(topic string) {
	var exist bool
	var old interface{}
	old, exist = b.subNumMap.Load(topic)
	if exist {
		ol, ok := old.(int)
		if ok {
			if ol > 1 {
				b.subNumMap.Store(topic, ol-1)
			} else {
				b.subNumMap.Delete(topic)
				b.actionElementChan <- topics_p2p.ActionElement{Operate: topics_p2p.UnSubscribe4P2POperateCode, Topic: []byte(topic)}
			}
		}
	}
}

func (b *BrokerP2PNode) RegisterDeliverForwardPacketsToTargetNode(f func(string, string, []packets.PublishPacket)) {
	b.deliverForwardPacketsToTargetNode = f
}

func (b *BrokerP2PNode) RegisterDeliverTopicActionsToPeerNodes(f func(*Broker, []topics_p2p.ActionElement)) {
	b.deliverTopicActionsToPeerNodes = f
}

// **********************
// Still for broker ...

func (b *Broker) BrokerID() xid.ID {
	return *b.brokerNode.brokerID
}

func (b *Broker) Overlay() *kademlia.Protocol {
	return b.brokerNode.overlay
}

func (b *Broker) SetOverlay(overlay *kademlia.Protocol) {
	b.brokerNode.SetOverlay(overlay)
}

func (b *Broker) NodeID() cryptographic.ID {
	return *b.brokerNode.nodeID
}

func (b *Broker) Node() *p2p.Node {
	return b.node
}

func (b *Broker) BrokerNode() *BrokerP2PNode {
	return b.brokerNode
}

func (b *Broker) TopicsManager4P2P() *topics_p2p.Manager4P2P {
	return b.topicsManager4P2P
}

func (b *Broker) ExistedTopicList() []string {
	topicList := make([]string, 0, defaultTopicListCapacity)
	b.brokerNode.subNumMap.Range(func(ki, vi interface{}) bool {
		k, _ := ki.(string), vi.(int)
		topicList = append(topicList, k)
		return true
	})
	return topicList
}

func (b *Broker) ExistedTopicActions() []topics_p2p.ActionElement {
	aeList := make([]topics_p2p.ActionElement, 0, defaultActionElementListCapacity)
	b.brokerNode.subNumMap.Range(func(ki, vi interface{}) bool {
		k, _ := ki.(string), vi.(int)
		aeList = append(aeList, topics_p2p.ActionElement{Operate: topics_p2p.Subscribe4P2POperateCode, Topic: []byte(k)})
		return true
	})
	return aeList
}

// This will be called by StartListening
func (b *Broker) startProcessActionElementListTask() {
	go func() {
		lastTime := time.Now()
		lastTime4Notification := time.Now()

		aeList := make([]topics_p2p.ActionElement, 0, defaultActionElementListCapacity)
		infoList := make([]string, 0, defaultInfoListCapacity)
		for ae := range b.brokerNode.actionElementChan {
			aeList = append(aeList, ae)

			if time.Since(lastTime).Milliseconds() > defaultActionElementListAcceptTimeInterval || len(aeList) >= defaultActionElementListCapacity {
				b.brokerNode.deliverTopicActionsToPeerNodes(b, aeList)

				info := fmt.Sprintf(`{"amount":%d,"interval":"%s"}`,
					len(aeList),
					humanize.SI(time.Since(lastTime).Seconds(), "s"),
				)
				infoList = append(infoList, info)

				if time.Since(lastTime4Notification).Seconds() > defaultTopicActionsNotificationTimeInterval {
					b.TopicActionsMetricsNotification(b.brokerNode.brokerID.String(), infoList)
					infoList = make([]string, 0, defaultInfoListCapacity)
					lastTime4Notification = time.Now()
				}

				aeList = make([]topics_p2p.ActionElement, 0, defaultActionElementListCapacity)
				lastTime = time.Now()
			}
		}
	}()
}

// This will be called by StartListening
func (b *Broker) startCandidateForwardConfirmTask() {
	go func() {
		for pkt := range b.brokerNode.candidateForwardConfirmChan {
			b.brokerNode.packetForwardMetrics.increasingNumOfCandidate()

			var brokerIdStrList []interface{}
			b.brokerNode.mu.Lock()
			err := b.topicsManager4P2P.Brokers4P2P([]byte(pkt.TopicName), &brokerIdStrList)
			b.brokerNode.mu.Unlock()

			if err != nil {
				b.brokerNode.packetForwardMetrics.increasingNumOfTopicMatchBrokerFailed()
			} else {
				if len(brokerIdStrList) > 0 {
					for _, broker := range brokerIdStrList {
						brokerIdStr, ok := broker.(string)
						if ok && len(brokerIdStr) > 0 {
							b.processForwardPacket(brokerIdStr, pkt)
						}
					}
					b.brokerNode.packetForwardMetrics.increasingNumOfForwarding()
				}
			}
		}
	}()
}

func (b *Broker) processForwardPacket(targetBrokerIdStr string, pkt *packets.PublishPacket) {
	var forwardMessageChan, exist = b.brokerNode.forwardPacketChanMap[targetBrokerIdStr]
	if !exist {
		forwardMessageChan = make(chan *packets.PublishPacket, defaultForwardPacketChanSize)

		b.brokerNode.mu.Lock()
		b.brokerNode.forwardPacketChanMap[targetBrokerIdStr] = forwardMessageChan
		b.brokerNode.mu.Unlock()

		b.startProcessForwardMessageTask(targetBrokerIdStr, forwardMessageChan)

		//wait for the task done.
		time.Sleep(500 * time.Millisecond)
	}

	forwardMessageChan <- pkt
	b.brokerNode.packetForwardMetrics.increasingNumOfForwardToMultiBroker()
}

// This will be called by processForwardMessage
func (b *Broker) startProcessForwardMessageTask(targetBrokerIdStr string, fmChan chan *packets.PublishPacket) {
	var targetNodeIdAddr = b.brokerNode.NodeIdAddrGetFromMap(targetBrokerIdStr)
	go func() {
		lastTime := time.Now()
		lastTime4Notification := time.Now()

		pkList := make([]packets.PublishPacket, 0, defaultForwardPacketListCapacity)
		infoList := make([]string, 0, defaultInfoListCapacity)
		var info string
		for pkt := range fmChan {
			pkList = append(pkList, *pkt)
			b.logger.Debug("Received the forward message",
				zap.String("topic", pkt.TopicName),
				zap.Int("payload size", len(pkt.Payload)),
				zap.String("target broker id", targetBrokerIdStr),
				zap.String("target node id address", targetNodeIdAddr),
			)

			if time.Since(lastTime).Milliseconds() > defaultForwardPacketListAcceptTimeInterval || len(pkList) >= defaultForwardPacketListCapacity {
				if len(targetNodeIdAddr) < 1 || targetNodeIdAddr == "ERROR" {
					targetNodeIdAddr = b.brokerNode.NodeIdAddrGetFromMap(targetBrokerIdStr)
					time.Sleep(50 * time.Millisecond)
				}
				if len(targetNodeIdAddr) > 0 && targetNodeIdAddr != "ERROR" {
					b.brokerNode.deliverForwardPacketsToTargetNode(targetNodeIdAddr, targetBrokerIdStr, pkList)
					b.brokerNode.packetForwardMetrics.increasingNumOfForwardParcelOverP2P()

					info = fmt.Sprintf(`{"amount":%d,"interval":"%s"}`,
						len(pkList),
						humanize.SI(time.Since(lastTime).Seconds(), "s"),
					)

					pkList = make([]packets.PublishPacket, 0, defaultForwardPacketListCapacity)
					lastTime = time.Now()
				} else {
					info = fmt.Sprintf(`{"waiting_amount":%d,"waiting_interval":"%s"}`,
						len(pkList),
						humanize.SI(time.Since(lastTime).Seconds(), "s"),
					)
					//Todo close the channel of the target_broker_id
				}
				infoList = append(infoList, info)
				b.logger.Debug("Deliver Forward Packets To Target Node ", zap.String("metrics", info))

				if time.Since(lastTime4Notification).Seconds() > defaultForwardPacketsNotificationTimeInterval {
					b.ForwardPacketsMetricsNotification(b.BrokerID().String(), targetBrokerIdStr, infoList)
					infoList = make([]string, 0, defaultInfoListCapacity)
					lastTime4Notification = time.Now()
				}
			}
		}
	}()
}
