package main

import (
	"net"
	"runtime"

	p2p "awesomeProject/beacon/p2p_network/core_module"
	"awesomeProject/beacon/p2p_network/libs/cryptographic"
	"awesomeProject/beacon/p2p_network/libs/kademlia"

	"go.uber.org/zap"
)

func serviceWithFlag(host net.IP, port uint16, address string, addresses ...string) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	theLogger, err := zap.NewDevelopment(zap.AddStacktrace(zap.InfoLevel))
	checkForPanics(err)
	defer theLogger.Sync()

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

	// Register the MessageOverP2P Go type to the node with an associated unmarshal function.
	node.RegisterMessage(messageOverP2P{}, unmarshalMessageOverP2P)

	// Register a message handler to the node.
	node.Handle(handle)

	// Instantiate Kademlia.
	events := kademlia.Events{
		OnPeerAdmitted: func(id cryptographic.ID) {
			theLogger.Info("Learned about a new peer node ",
				zap.String("Address", id.Address),
				zap.String("Public Key", id.PubKey.String()[:PrintedLength]),
			)
			msg := messageOverP2P{contents: "hello,alice"}
			err = msg.SendMessageOverP2PToTargetNode(node, id)
		},
		OnPeerEvicted: func(id cryptographic.ID) {
			theLogger.Info("Forgotten a new peer node ",
				zap.String("Address", id.Address),
				zap.String("Public Key", id.PubKey.String()[:PrintedLength]),
			)
		},
	}

	overlay := kademlia.New(kademlia.WithProtocolEvents(events))

	// Bind Kademlia to the node.
	node.Bind(overlay.Protocol())

	// Have the node start listening for new peers.
	checkForPanics(node.Listen())

	// Ping nodes to initially bootstrap and discover peers from.
	bootstrap(node, addresses...)

	// Start the p2p node task
	startP2PNodeTask(theLogger, overlay)

	waitForSignal()
}
