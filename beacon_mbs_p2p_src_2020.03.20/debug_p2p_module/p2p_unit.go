package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	p2p "awesomeProject/beacon/p2p_network/core_module"
	"awesomeProject/beacon/p2p_network/libs/kademlia"
	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

// printedLength is the total prefix length of a public key associated to a node ID.
const PrintedLength = 8

// metrics
var (
	sentMessageOverP2P       = uint32(0)
	sentMessageFailedOverP2P = uint32(0)
	receivedMessageOverP2P   = uint32(0)
)

// check panics if err is not nil.
func checkForPanics(err error) {
	if err != nil {
		panic(err)
	}
}
func waitForSignal() {
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Kill, os.Interrupt)
	<-signalChan
	signal.Stop(signalChan)
}

// bootstrap pings and dials an array of network addresses which we may interact with and discover peers from.
func bootstrap(node *p2p.Node, addresses ...string) {
	for _, addr := range addresses {
		node.Logger().Debug("mqtt_service_p2p/node_service bootstrap ", zap.String("flag address", addr))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := node.Ping(ctx, addr)

		cancel()

		if err != nil {
			//fmt.Printf("Failed to ping bootstrap node (%s). Skipping... [error: %s]\n", addr, err)
			node.Logger().Error("Failed to ping bootstrap node. Skipping ... ",
				zap.Error(err),
				zap.String("bootstrap node address", addr),
			)
			continue
		} else {
			node.Logger().Debug("Succeed to ping bootstrap node.",
				zap.String("bootstrap node address", addr),
			)
		}
	}
}

// discover uses Kademlia to discover new peers from nodes we already are aware of.
func discover(logger *zap.Logger, overlay *kademlia.Protocol) string {
	ids := overlay.Discover()

	var str []string
	var info string
	for _, id := range ids {
		str = append(str, fmt.Sprintf("%s(%s)", id.Address, id.PubKey.String()[:PrintedLength]))
	}

	if len(ids) > 0 {
		logger.Info(fmt.Sprintf("Discovered %d peer(s)", len(ids)),
			zap.String("detail", strings.Join(str, ", ")),
		)
		info = fmt.Sprintf(`{"discovered_peers":%d,"detail":"%s"}`, len(ids), strings.Join(str, ","))
	} else {
		logger.Error("Did not discover any peers.")
		info = `{"discovered_peers":0,"detail":""}`
	}
	return info
}

// peers prints out all peers we are already aware of.
func peers(logger *zap.Logger, overlay *kademlia.Protocol) string {
	ids := overlay.Table().Peers()

	var str []string
	var info string
	for _, id := range ids {
		str = append(str, fmt.Sprintf("%s(%s)", id.Address, id.PubKey.String()[:PrintedLength]))
	}

	if len(ids) > 0 {
		logger.Info(fmt.Sprintf("Node has(have) known %d peer(s)", len(ids)),
			zap.String("detail", strings.Join(str, ", ")),
		)
		info = fmt.Sprintf(`{"know_peers":%d,"detail":"%s"}`, len(ids), strings.Join(str, ","))
	} else {
		logger.Error("Node did not know any peers.")
		info = `{"know_peers":0,"detail":""}`
	}
	return info
}

// handle handles and prints out valid messages from peers.
func handle(ctx p2p.HandlerContext) error {
	if ctx.IsRequest() {
		ctx.Logger().Debug("node_service/handle : Information ",
			zap.String("address", ctx.ID().Address),
			zap.String("public key", ctx.ID().PubKey.String()[:PrintedLength]),
			zap.String("handler context", "is request"),
		)
		return nil
	}

	obj, err := ctx.DecodeMessage()
	if err != nil {
		return nil
	}

	msg, ok := obj.(*messageOverP2P)
	if !ok {
		return nil
	}

	if len(msg.contents) == 0 {
		return nil
	}

	atomic.AddUint32(&receivedMessageOverP2P, 1)

	ctx.Logger().Debug("node_service/handle : Information ",
		zap.String("address", ctx.ID().Address),
		zap.String("Public Key", ctx.ID().PubKey.String()[:PrintedLength]),
		zap.String("Content Size", humanize.Bytes(uint64(len(msg.contents)))),
	)

	return nil
}

func startP2PNodeTask(logger *zap.Logger, overlay *kademlia.Protocol) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			// Attempt to discover peers if we are bootstrapped to any nodes.
			_ = discover(logger, overlay)

			// Attempt to show all peers we are already aware of.
			_ = peers(logger, overlay)
		}
	}()
}
