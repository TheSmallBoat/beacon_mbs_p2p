package broker_p2p_module

import (
	"fmt"
	"net"
	"runtime"

	"awesomeProject/beacon/general_toolbox/logger"

	mqtt "awesomeProject/beacon/mqtt_network/broker_core_module"

	p2p "awesomeProject/beacon/p2p_network/core_module"
	"awesomeProject/beacon/p2p_network/libs/cryptographic"
	"awesomeProject/beacon/p2p_network/libs/kademlia"

	"go.uber.org/zap"
)

func ServiceWithFlag(host net.IP, port uint16, address string, mHost net.IP, mPort uint16, mAddress string, debug bool, addresses ...string) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	logger.InitLogger(debug, "mqtt_service_p2p")
	theLogger := logger.Get()

	// Node
	node, errP2P := p2p.NewNode(
		p2p.WithNodeLogger(theLogger),
		p2p.WithNodeBindHost(host),
		p2p.WithNodeBindPort(port),
		p2p.WithNodeAddress(address),
	)
	checkForPanics(errP2P)

	// Release resources associated to node at the end of the program.
	defer node.Close()

	// Broker
	broker, errMQTT := mqtt.NewBroker(
		mqtt.WithBrokerLogger(theLogger),
		mqtt.WithBrokerBindHost(mHost),
		mqtt.WithBrokerBindPort(mPort),
		mqtt.WithBrokerAddress(mAddress),
		mqtt.WithNodeId(node.ID()),
		mqtt.WithNode(node),
	)
	checkForPanics(errMQTT)

	// Register the MessageOverP2P Go type to the node with an associated unmarshal function.
	node.RegisterMessage(MessageOverP2P{}, UnmarshalMessageOverP2P)

	// Set NodeIdsInfoMsgOverP2PForSwap for swap NodeIdsInfo by handler.
	setNodeIdsInfoMsgOverP2PForSwap(broker)

	// Register a message handler to the node.
	node.Handle(handleSend, handleRequest)

	// Instantiate Kademlia.
	events := kademlia.Events{
		OnPeerAdmitted: func(id cryptographic.ID) {
			theLogger.Info("Learned about a new peer node ",
				zap.String("Address", id.Address),
				zap.String("Public Key", id.PubKey.String()[:PrintedLength]),
			)

			processExistedTopicsAndDeliverToTargetNodeAtOnce(broker, id.Address)
		},
		OnPeerEvicted: func(id cryptographic.ID) {
			theLogger.Info("Forgotten a new peer node ",
				zap.String("Address", id.Address),
				zap.String("Public Key", id.PubKey.String()[:PrintedLength]),
			)

			info := fmt.Sprintf(`{"peer_node_address":"%s","public_key":"%s"}`, id.Address, id.PubKey.String()[:PrintedLength])
			broker.PeerNodeNotification(broker.BrokerID().String(), "peer_evicted", info)

			//Todo process the target id related task. such as remove NodeIdsInfo,remove target's topics in this node.
		},
	}

	overlay := kademlia.New(kademlia.WithProtocolEvents(events))

	// set overlay to broker
	broker.SetOverlay(overlay)

	// Register DeliverForwardPacketsToTargetNode
	broker.BrokerNode().RegisterDeliverForwardPacketsToTargetNode(deliverForwardPacketsToTargetNode)
	broker.BrokerNode().RegisterDeliverTopicActionsToPeerNodes(deliverTopicActionsToPeerNodes)

	// Bind Kademlia to the node.
	node.Bind(overlay.Protocol())

	// Start the processing pending message parcel job task
	startProcessPendingMessageParcelJobTask(theLogger, node)

	// Start the processing received message over p2p job task
	startProcessReceivedMessageOverP2PJobTask(theLogger, broker)

	// Start the metrics job task
	startMetricsJobTask(broker)

	// Have the node start listening for new peers.
	checkForPanics(node.Listen())

	// Ping nodes to initially bootstrap and discover peers from.
	go bootstrap(broker, node, overlay, addresses...)

	// Have the broker start listening.
	checkForPanics(broker.StartListening())

	waitForSignal()
}
