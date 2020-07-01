package broker_core_module

import (
	"net"

	"awesomeProject/beacon/general_toolbox/logger"

	"awesomeProject/beacon/mqtt_network/libs/pool"
	"awesomeProject/beacon/mqtt_network/libs/sessions"
	"awesomeProject/beacon/mqtt_network/libs/topics"
	"awesomeProject/beacon/mqtt_network/libs/topics_p2p"

	p2p "awesomeProject/beacon/p2p_network/core_module"
	"awesomeProject/beacon/p2p_network/libs/cryptographic"

	"go.uber.org/zap"
)

// BrokerOption represents a functional option that may be passed to NewBroker for instantiating a new broker instance
// with configured values.
type BrokerOption func(b *Broker)

// WithNodeLogger sets the logger implementation that the broker shall use.
// By default, zap.NewNop() is assigned which disables any logs.
func WithBrokerLogger(log *zap.Logger) BrokerOption {
	return func(b *Broker) {
		if log == nil {
			log = logger.Get()
		}

		b.logger = log
	}
}

func WithFixedWorkPool(maxWorkers uint16) BrokerOption {
	return func(b *Broker) {
		b.fixedWorkPool = pool.NewFixedWorkPool(maxWorkers)
	}
}

func WithTopicsManager(providerName string) BrokerOption {
	return func(b *Broker) {
		b.topicsManager, _ = topics.NewManager(providerName)
	}
}

func WithSessionsManager(providerName string) BrokerOption {
	return func(b *Broker) {
		b.sessionManager, _ = sessions.NewManager(providerName)
	}
}

func WithTopicsManager4P2P(providerName string) BrokerOption {
	return func(b *Broker) {
		b.topicsManager4P2P, _ = topics_p2p.NewManager4P2P(providerName)
	}
}

func WithBrokerBindHost(host net.IP) BrokerOption {
	return func(b *Broker) {
		b.host = host
	}
}

func WithBrokerBindPort(port uint16) BrokerOption {
	return func(b *Broker) {
		b.port = port
	}
}

func WithBrokerAddress(address string) BrokerOption {
	return func(b *Broker) {
		b.addr = address
	}
}

func WithBrokerP2PNode(bpn *BrokerP2PNode) BrokerOption {
	return func(b *Broker) {
		if bpn == nil {
			bpn = NewBrokerP2PNode()
		}
		b.brokerNode = bpn
	}
}

func WithNodeId(nodeID cryptographic.ID) BrokerOption {
	return func(b *Broker) {
		if b.brokerNode == nil {
			b.brokerNode = NewBrokerP2PNode()
		}
		b.brokerNode.SetNodeId(&nodeID)
	}
}

func WithNode(node *p2p.Node) BrokerOption {
	return func(b *Broker) {
		b.node = node
	}
}
