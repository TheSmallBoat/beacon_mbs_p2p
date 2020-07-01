package broker_core_module

import (
	"errors"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"

	"awesomeProject/beacon/general_toolbox/logger"

	"awesomeProject/beacon/mqtt_network/libs/pool"
	"awesomeProject/beacon/mqtt_network/libs/sessions"
	"awesomeProject/beacon/mqtt_network/libs/topics"
	"awesomeProject/beacon/mqtt_network/libs/topics_p2p"

	p2p "awesomeProject/beacon/p2p_network/core_module"
	"awesomeProject/beacon/p2p_network/libs/common"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// ACCEPT_MIN_SLEEP is the minimum acceptable sleep times on temporary errors.
	AcceptMinSleep = 100 * time.Millisecond

	// ACCEPT_MAX_SLEEP is the maximum acceptable sleep times on temporary errors
	AcceptMaxSleep = 10 * time.Second

	defaultMQTTBindPort = 1883
	defaultMQTTBindHost = "127.0.0.1"
)

const (
	QosAtMostOnce byte = iota
	QosAtLeastOnce
	QosExactlyOnce
	QosFailure = 0x80
)

type Message struct {
	client *client
	packet packets.ControlPacket
}

type Broker struct {
	mu     sync.Mutex
	logger *zap.Logger

	fixedWorkPool     *pool.FixedWorkPool
	topicsManager     *topics.Manager
	sessionManager    *sessions.Manager
	topicsManager4P2P *topics_p2p.Manager4P2P

	brokerNode *BrokerP2PNode
	node       *p2p.Node

	host net.IP
	port uint16
	addr string

	clients sync.Map

	listener  net.Listener
	listening atomic.Bool
}

type subscription struct {
	client    *client
	topic     string
	qos       byte
	share     bool
	groupName string
}

func NewBroker(opts ...BrokerOption) (*Broker, error) {
	b := &Broker{
		mu:   sync.Mutex{},
		host: net.ParseIP(defaultMQTTBindHost),
		port: defaultMQTTBindPort,
	}

	for _, opt := range opts {
		opt(b)
	}

	if b.node == nil {
		err := errors.New("broker need to set node pointer, and cannot be nil, please check it ")
		panic(err)
	}

	if b.brokerNode == nil {
		b.brokerNode = NewBrokerP2PNode()
	}

	if b.logger == nil {
		logger.InitLogger(false, b.brokerNode.brokerID.String())
		b.logger = logger.Get()
	}

	if b.fixedWorkPool == nil {
		b.fixedWorkPool = pool.NewFixedWorkPool(uint16(runtime.NumCPU()))
	}

	var err error

	if b.topicsManager == nil {
		topics.RegisterMemTopicsProvider()
		b.topicsManager, err = topics.NewManager("mem")
		if err != nil {
			return nil, err
		}
	}

	if b.topicsManager4P2P == nil {
		topics_p2p.RegisterMemTopicsProvider4P2P()
		b.topicsManager4P2P, err = topics_p2p.NewManager4P2P("mem")
		if err != nil {
			return nil, err
		}
	}

	if b.sessionManager == nil {
		sessions.RegisterMemSessionProvider()
		b.sessionManager, err = sessions.NewManager("mem")
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (b *Broker) SubmitWorkTask(msg *Message) {
	b.fixedWorkPool.SubmitTask(func() {
		ProcessMessage(msg)
	})
}

func (b *Broker) StartListening() error {
	if b.listening.Load() {
		return errors.New("broker is already listening")
	}

	var err error
	defer func() {
		if err != nil {
			b.listening.Store(false)
		}
	}()

	err = b.brokerNode.checkNil()
	if err != nil {
		return err
	}

	b.listener, err = net.Listen("tcp", net.JoinHostPort(common.NormalizeIP(b.host), strconv.FormatUint(uint64(b.port), 10)))
	if err != nil {
		return err
	}

	addr, ok := b.listener.Addr().(*net.TCPAddr)
	if !ok {
		_ = b.listener.Close()
		return errors.New("did not bind to a tcp addr")
	}

	b.host = addr.IP
	b.port = uint16(addr.Port)

	if b.addr == "" {
		b.addr = net.JoinHostPort(common.NormalizeIP(b.host), strconv.FormatUint(uint64(b.port), 10))
	} else {
		resolved, err := common.ResolveAddress(b.addr)
		if err != nil {
			_ = b.listener.Close()
			return err
		}

		hostStr, portStr, err := net.SplitHostPort(resolved)
		if err != nil {
			_ = b.listener.Close()
			return err
		}

		host := net.ParseIP(hostStr)
		if host == nil {
			_ = b.listener.Close()
			return errors.New("host in provided public address is invalid (must be IPv4/IPv6)")
		}

		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil || uint16(port) != b.port {
			_ = b.listener.Close()
			return err
		}
	}

	b.listening.Store(true)
	b.logger.Info("Listening for mqtt broker.",
		zap.String("bind_addr", addr.String()),
		zap.String("broker_id", b.BrokerID().String()),
		zap.String("p2p_addr", b.NodeID().Address),
		zap.String("p2p_public_key", b.NodeID().PubKey.String()),
	)

	// Do some tasks.
	b.startMetricsNotificationTask()
	b.startCandidateForwardConfirmTask()
	b.startProcessActionElementListTask()

	// Handle connections.
	tmpDelay := 10 * AcceptMinSleep
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				b.logger.Error("Temporary MQTT client accept error on listening",
					zap.Error(ne),
					zap.Duration("sleeping(ms)", tmpDelay/time.Millisecond),
				)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > AcceptMaxSleep {
					tmpDelay = AcceptMaxSleep
				}
			} else {
				b.logger.Error("MQTT client accept error on listening", zap.Error(err))
			}
			continue
		}
		tmpDelay = AcceptMinSleep
		go b.handleConnection(conn)
	}
}

func (b *Broker) handleConnection(conn net.Conn) {
	//process connect packet
	packet, err := packets.ReadPacket(conn)
	if err != nil {
		b.logger.Error("core_module/broker/handleConnection: read connect packet error => ", zap.Error(err))
		return
	}
	if packet == nil {
		b.logger.Error("core_module/broker/handleConnection: received nil packet")
		return
	}
	msg, ok := packet.(*packets.ConnectPacket)
	if !ok {
		b.logger.Error("core_module/broker/handleConnection: received msg that was not Connect")
		return
	}
	b.logger.Info("core_module/broker/handleConnection: read connect from ",
		zap.String("clientID", msg.ClientIdentifier))

	connAck := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connAck.SessionPresent = msg.CleanSession
	connAck.ReturnCode = msg.Validate()

	if connAck.ReturnCode != packets.Accepted {
		err = connAck.Write(conn)
		if err != nil {
			b.logger.Error("core_module/broker/handleConnection: send connAck error, ",
				zap.Error(err),
				zap.String("clientID", msg.ClientIdentifier),
			)
			return
		}
		return
	}

	// TODO CheckConnectAuth

	err = connAck.Write(conn)
	if err != nil {
		b.logger.Error("core_module/broker/handleConnection: send connAck error, ",
			zap.Error(err),
			zap.String("clientID", msg.ClientIdentifier))
		return
	}

	var willMsg *packets.PublishPacket
	if msg.WillFlag {
		willMsg = packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		willMsg.Qos = msg.WillQos
		willMsg.TopicName = msg.WillTopic
		willMsg.Retain = msg.WillRetain
		willMsg.Payload = msg.WillMessage
		willMsg.Dup = msg.Dup
	} else {
		willMsg = nil
	}

	info := info{
		clientID:    msg.ClientIdentifier,
		username:    msg.Username,
		password:    msg.Password,
		keepalive:   msg.Keepalive,
		willMessage: willMsg,
	}

	c := &client{
		broker: b,
		conn:   conn,
		info:   info,
	}

	c.init()

	err = b.getSession(c, msg, connAck)
	if err != nil {
		b.logger.Error("core_module/broker/handleConnection: get session error => ",
			zap.Error(err),
			zap.String("clientID", c.info.clientID),
		)
		return
	}

	cid := c.info.clientID

	var exist bool
	var old interface{}

	old, exist = b.clients.Load(cid)
	if exist {
		b.logger.Warn("core_module/broker/handleConnection: client exist, close old ...",
			zap.String("clientID", c.info.clientID),
		)
		ol, ok := old.(*client)
		if ok {
			ol.Close()
		}
	}
	b.clients.Store(cid, c)
	b.OnlineOfflineNotification(cid, true)

	c.readLoop()
}

func (b *Broker) removeClient(c *client) {
	clientId := c.info.clientID
	if clientId != "" {
		b.clients.Delete(clientId)
		b.logger.Info("core_module/broker/removeClient: delete client ,",
			zap.String("ClientID", clientId),
		)
	}
}

// The PublishPacket is not from the client, may be created by the broker for publishing it to the client.
// Also, this can be used to forward packets from the peer node to clients.
func PublishMessageWithBroker(b *Broker, packet *packets.PublishPacket) {
	var subList []interface{}
	var qosList []byte

	b.mu.Lock()
	err := b.topicsManager.Subscribers([]byte(packet.TopicName), packet.Qos, &subList, &qosList)
	b.mu.Unlock()

	if err != nil {
		b.logger.Error("core_module/broker/PublishMessage: search subscribe client error.",
			zap.Error(err),
		)
		return
	}

	if len(subList) == 0 {
		return
	}

	var qSub []int
	for i, sub := range subList {
		s, ok := sub.(*subscription)
		if ok {
			if s.share {
				qSub = append(qSub, i)
			} else {
				err := s.client.WriterPacket(packet)
				if err != nil {
					b.logger.Error("core_module/broker/PublishMessage: Error publish to subscriber => ",
						zap.Error(err),
						zap.String("ClientID", s.client.info.clientID),
					)
				}
			}
		}
	}

	if len(qSub) > 0 {
		idx := r.Intn(len(qSub))
		sub := subList[qSub[idx]].(*subscription)
		err := sub.client.WriterPacket(packet)
		if err != nil {
			b.logger.Error("core_module/broker/PublishMessage: Error publish to subscriber [share group random mode] => ",
				zap.Error(err),
				zap.String("ClientID", sub.client.info.clientID),
			)
		}
	}
}

func (b *Broker) SubmitPublishPacketsWorkTask(packet *packets.PublishPacket) {
	// This can work after the broker start listening
	if b.listening.Load() {
		b.fixedWorkPool.SubmitTask(func() {
			PublishMessageWithBroker(b, packet)
		})
		b.fixedWorkPool.Metrics().IncreasingPublishPacketTaskSubmitted()
	}
}

func (b *Broker) startMetricsNotificationTask() {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			b.WorkPoolMetricsNotification(b.BrokerID().String(), b.fixedWorkPool.Metrics().MetricsInfo())
			b.PacketForwardMetricsNotification(b.BrokerID().String(), b.brokerNode.packetForwardMetrics.MetricsInfo())
		}
	}()
}
