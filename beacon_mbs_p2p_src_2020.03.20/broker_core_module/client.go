package broker_core_module

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"awesomeProject/beacon/general_toolbox/logger"

	"awesomeProject/beacon/mqtt_network/libs/sessions"
	"awesomeProject/beacon/mqtt_network/libs/topics"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"go.uber.org/zap"
)

const (
	_GroupTopicRegexp = `^\$share/([0-9a-zA-Z_-]+)/(.*)$`
)

const (
	Connected    = true
	Disconnected = false
)

var (
	groupCompile = regexp.MustCompile(_GroupTopicRegexp)

	r = rand.New(rand.NewSource(time.Now().UnixNano()))
)

type client struct {
	mu     sync.Mutex
	logger *zap.Logger

	conn net.Conn

	broker *Broker
	info   info
	status bool

	ctx             context.Context
	cancelFunc      context.CancelFunc
	session         *sessions.Session
	topicsManager   *topics.Manager
	subscriptionMap map[string]*subscription

	subList             []interface{}
	qosList             []byte
	retainedMessageList []*packets.PublishPacket
}

type info struct {
	clientID    string
	username    string
	password    []byte
	keepalive   uint16
	willMessage *packets.PublishPacket
	localIP     string
	remoteIP    string
}

func (c *client) init() {
	c.mu = sync.Mutex{}
	c.status = Connected

	c.info.localIP = strings.Split(c.conn.LocalAddr().String(), ":")[0]
	c.info.remoteIP = strings.Split(c.conn.RemoteAddr().String(), ":")[0]

	c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	c.subscriptionMap = make(map[string]*subscription)

	c.topicsManager = c.broker.topicsManager

	c.logger = logger.Get()
}

func (c *client) readLoop() {
	nc := c.conn
	b := c.broker
	if nc == nil || b == nil {
		return
	}

	keepAlive := time.Second * time.Duration(c.info.keepalive)
	timeOut := keepAlive + (keepAlive / 2)

	defer c.Close()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			//add read timeout
			if err := nc.SetReadDeadline(time.Now().Add(timeOut)); err != nil {
				c.logger.Error("core_module/client/readLoop: set read timeout error => ",
					zap.Error(err),
					zap.String("ClientID", c.info.clientID),
				)
				return
			}

			packet, err := packets.ReadPacket(nc)
			if err != nil {
				if errors.Is(err, io.EOF) {
					c.logger.Warn("core_module/client/readLoop: read packet io.EOF => ",
						zap.String("ClientID", c.info.clientID),
					)
				} else {
					c.logger.Error("core_module/client/readLoop: read packet error => ",
						zap.Error(err),
						zap.String("ClientID", c.info.clientID),
					)
				}
				return
			}
			msg := &Message{
				client: c,
				packet: packet,
			}
			b.SubmitWorkTask(msg)
		}
	}
}

func ProcessMessage(msg *Message) {
	c := msg.client
	ca := msg.packet
	if ca == nil {
		return
	}

	//log.Debug("client/ProcessMessage: Recv message => ", zap.String("message type", reflect.TypeOf(msg.packet).String()[9:]), zap.String("ClientID", c.info.clientID))

	switch ca.(type) {
	case *packets.ConnackPacket:
	case *packets.ConnectPacket:
	case *packets.PublishPacket:
		packet := ca.(*packets.PublishPacket)
		c.ProcessPublish(packet)
	case *packets.PubackPacket:
	case *packets.PubrecPacket:
	case *packets.PubrelPacket:
	case *packets.PubcompPacket:
	case *packets.SubscribePacket:
		packet := ca.(*packets.SubscribePacket)
		c.ProcessSubscribe(packet)
	case *packets.SubackPacket:
	case *packets.UnsubscribePacket:
		packet := ca.(*packets.UnsubscribePacket)
		c.ProcessUnSubscribe(packet)
	case *packets.UnsubackPacket:
	case *packets.PingreqPacket:
		c.ProcessPing()
	case *packets.PingrespPacket:
	case *packets.DisconnectPacket:
		c.Close()
	default:
		//log.Info("client/ProcessMessage: Recv unknown message .......", zap.String("ClientID", c.info.clientID))
	}
}

func (c *client) ProcessPublish(packet *packets.PublishPacket) {
	c.processClientPublish(packet)
}

func (c *client) processClientPublish(packet *packets.PublishPacket) {
	if c.status == Disconnected {
		return
	}

	// TODO CheckTopicAuth

	switch packet.Qos {
	case QosAtMostOnce:
		c.ProcessPublishMessage(packet)
	case QosAtLeastOnce:
		pubAck := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		pubAck.MessageID = packet.MessageID
		if err := c.WriterPacket(pubAck); err != nil {
			c.logger.Error("core_module/client/processClientPublish: send pubAck error, ",
				zap.Error(err),
				zap.String("ClientID", c.info.clientID),
			)
			return
		}
		c.ProcessPublishMessage(packet)
	case QosExactlyOnce:
		return
	default:
		c.logger.Error("core_module/client/processClientPublish: publish with unknown qos ",
			zap.String("ClientID", c.info.clientID),
		)
		return
	}
}

// The work pool will process the PublishPacket for this broker, and the other module that not in the work pool will forward it to other brokers.
func (c *client) ProcessPublishMessage(packet *packets.PublishPacket) {
	b := c.broker
	if b == nil {
		return
	}

	// it's very important section
	// put it to the candidate-forward-confirm channel
	b.brokerNode.candidateForwardConfirmChan <- packet

	if packet.Retain {
		if err := c.topicsManager.Retain(packet); err != nil {
			c.logger.Error("core_module/client/ProcessPublishMessage: Error retaining message => ",
				zap.Error(err),
				zap.String("ClientID", c.info.clientID),
			)
		}
	}
	c.mu.Lock()
	err := c.topicsManager.Subscribers([]byte(packet.TopicName), packet.Qos, &c.subList, &c.qosList)
	c.mu.Unlock()

	if err != nil {
		c.logger.Error("core_module/client/ProcessPublishMessage: Error retrieving subscribers list => ",
			zap.Error(err),
			zap.String("ClientID", c.info.clientID),
		)
		return
	}

	if len(c.subList) == 0 {
		return
	}

	var qSub []int
	for i, sub := range c.subList {
		s, ok := sub.(*subscription)
		if ok {
			if s.share {
				qSub = append(qSub, i)
			} else {
				err := s.client.WriterPacket(packet)
				if err != nil {
					c.logger.Error("core_module/client/ProcessPublishMessage: Error publish to subscriber => ",
						zap.Error(err),
						zap.String("ClientID", c.info.clientID),
					)
				}
			}
		}
	}

	if len(qSub) > 0 {
		idx := r.Intn(len(qSub))
		sub := c.subList[qSub[idx]].(*subscription)
		err := sub.client.WriterPacket(packet)
		if err != nil {
			c.logger.Error("core_module/client/ProcessPublishMessage: Error publish to subscriber [share group random mode] => ",
				zap.Error(err),
				zap.String("ClientID", c.info.clientID),
			)
		}
	}
}

func (c *client) ProcessSubscribe(packet *packets.SubscribePacket) {
	c.processClientSubscribe(packet)
}

func (c *client) processClientSubscribe(packet *packets.SubscribePacket) {
	if c.status == Disconnected {
		return
	}

	b := c.broker
	if b == nil {
		return
	}

	topicList := packet.Topics
	qosList := packet.Qoss

	subAck := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	subAck.MessageID = packet.MessageID
	var returnCodeList []byte

	for i, topic := range topicList {
		t := topic

		//TODO CheckTopicAuth

		groupName := ""
		share := false
		if strings.HasPrefix(topic, "$share/") {
			substr := groupCompile.FindStringSubmatch(topic)
			if len(substr) != 3 {
				returnCodeList = append(returnCodeList, QosFailure)
				continue
			}
			share = true
			groupName = substr[1]
			topic = substr[2]
		}

		sub := &subscription{
			client:    c,
			topic:     topic,
			qos:       qosList[i],
			share:     share,
			groupName: groupName,
		}

		returnQos, err := c.topicsManager.Subscribe([]byte(topic), qosList[i], sub)
		if err != nil {
			c.logger.Error("core_module/client/processClientSubscribe error, ",
				zap.Error(err),
				zap.String("ClientID", c.info.clientID),
			)
			returnCodeList = append(returnCodeList, QosFailure)
			continue
		}

		c.subscriptionMap[t] = sub

		_ = c.session.AddTopic(t, qosList[i])
		returnCodeList = append(returnCodeList, returnQos)
		_ = c.topicsManager.Retained([]byte(topic), &c.retainedMessageList)

		//process map for adding the subscriber number to the topic
		c.broker.brokerNode.ProcessSubNumMapForAdd(t)
	}

	subAck.ReturnCodes = returnCodeList
	err := c.WriterPacket(subAck)
	if err != nil {
		c.logger.Error("core_module/client/processClientSubscribe send subAck error, ",
			zap.Error(err),
			zap.String("ClientID", c.info.clientID),
		)
		return
	}

	//process retain message
	for _, rm := range c.retainedMessageList {
		if err := c.WriterPacket(rm); err != nil {
			c.logger.Error("core_module/client/processClientSubscribe: publishing retained message error, ",
				zap.Any("err", err),
				zap.String("ClientID", c.info.clientID),
			)
		} else {
			c.logger.Info("core_module/client/processClientSubscribe: process retain  message, ",
				zap.Any("packet", packet),
				zap.String("ClientID", c.info.clientID),
			)
		}
	}
}

func (c *client) ProcessUnSubscribe(packet *packets.UnsubscribePacket) {
	c.processClientUnSubscribe(packet)
}

func (c *client) processClientUnSubscribe(packet *packets.UnsubscribePacket) {
	if c.status == Disconnected {
		return
	}

	b := c.broker
	if b == nil {
		return
	}

	topicList := packet.Topics
	for _, topic := range topicList {
		sub, exist := c.subscriptionMap[topic]
		if exist {
			_ = c.topicsManager.Unsubscribe([]byte(sub.topic), sub)
			_ = c.session.RemoveTopic(topic)
			delete(c.subscriptionMap, topic)

			//process map for deleting the subscriber number to the topic
			c.broker.brokerNode.ProcessSubNumMapForDel(topic)
		}
	}

	unsubAck := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsubAck.MessageID = packet.MessageID

	err := c.WriterPacket(unsubAck)
	if err != nil {
		c.logger.Error("core_module/client/processClientUnSubscribe send unsubAck error, ",
			zap.Error(err),
			zap.String("ClientID", c.info.clientID),
		)
		return
	}
}

func (c *client) ProcessPing() {
	if c.status == Disconnected {
		return
	}
	ping := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	err := c.WriterPacket(ping)
	if err != nil {
		c.logger.Error("core_module/client/ProcessPing error, ",
			zap.Error(err),
			zap.String("ClientID", c.info.clientID),
		)
		return
	}
}

func (c *client) Close() {
	if c.status == Disconnected {
		return
	}

	c.cancelFunc()
	c.status = Disconnected

	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}

	b := c.broker
	subMap := c.subscriptionMap
	if b != nil {
		b.removeClient(c)
		for _, sub := range subMap {
			err := b.topicsManager.Unsubscribe([]byte(sub.topic), sub)
			if err != nil {
				c.logger.Error("core_module/client/Close unsubscribe error, ",
					zap.Error(err),
					zap.String("ClientID", c.info.clientID),
				)
			}

			//process map for deleting the subscriber number to the topic
			b.brokerNode.ProcessSubNumMapForDel(sub.topic)
		}

		//offline notification
		b.OnlineOfflineNotification(c.info.clientID, false)

		if c.info.willMessage != nil {
			b.SubmitPublishPacketsWorkTask(c.info.willMessage)
		}
	}
}

func (c *client) WriterPacket(packet packets.ControlPacket) error {
	if c.status == Disconnected {
		return nil
	}

	if packet == nil {
		return nil
	}
	if c.conn == nil {
		c.Close()
		return errors.New("core_module/client/WriterPacket: connection lost")
	}

	c.mu.Lock()
	err := packet.Write(c.conn)
	c.mu.Unlock()
	return err
}
