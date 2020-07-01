package broker_p2p_module

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"time"

	mqtt "awesomeProject/beacon/mqtt_network/broker_core_module"

	p2p "awesomeProject/beacon/p2p_network/core_module"
	"awesomeProject/beacon/p2p_network/libs/kademlia"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

// printedLength is the total prefix length of a public key associated to a node ID.
const PrintedLength = 8

// metrics
var (
	sentMessageOverP2P           = uint32(0)
	sentMessageFailedOverP2P     = uint32(0)
	receivedMessageOverP2P       = uint32(0)
	receivedMessageFailedOverP2P = uint32(0)
	swapNodeIdsInfoWithPeer      = uint32(0)
	executeMessageOverP2P        = uint32(0)
	executeMessageFailedOverP2P  = uint32(0)

	sentMessageOverP2PGrandTotal           = uint32(0)
	sentMessageFailedOverP2PGrandTotal     = uint32(0)
	receivedMessageOverP2PGrandTotal       = uint32(0)
	receivedMessageFailedOverP2PGrandTotal = uint32(0)
	swapNodeIdsInfoWithPeerGrandTotal      = uint32(0)
	executeMessageOverP2PGrandTotal        = uint32(0)
	executeMessageFailedOverP2PGrandTotal  = uint32(0)

	sentMessageByteOverP2P               = uint32(0)
	sentMessageByteOverP2PGrandTotal     = uint32(0)
	receivedMessageByteOverP2P           = uint32(0)
	receivedMessageByteOverP2PGrandTotal = uint32(0)

	requestHandleOverP2P               = uint32(0)
	requestHandleOverP2PGrandTotal     = uint32(0)
	requestHandleByteOverP2P           = uint32(0)
	requestHandleByteOverP2PGrandTotal = uint32(0)
)

var ReceivedMessageOverP2PChan = make(chan *MessageOverP2P, 64)

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
	//return s
}

// bootstrap pings and dials an array of network addresses which we may interact with and discover peers from.
func bootstrap(broker *mqtt.Broker, node *p2p.Node, overlay *kademlia.Protocol, addresses ...string) {
	for _, addr := range addresses {
		node.Logger().Debug("mqtt_service_p2p/node_service bootstrap ", zap.String("flag address", addr))

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := node.Ping(ctx, addr)

		cancel()

		if err != nil {
			node.Logger().Error("Failed to ping bootstrap node. Skipping ... ",
				zap.Error(err),
				zap.String("bootstrap node address", addr),
			)
			continue
		} else {
			node.Logger().Debug("Succeed to ping bootstrap node.", zap.String("bootstrap node address", addr))
			swapNodeIdsInfoWithPeerNode(broker, addr)
		}
	}
	discover(node.Logger(), overlay)
	processExistedTopicsAndDeliverToPeerNodes(broker)
}

// discover uses Kademlia to discover new peers from nodes we already are aware of.
func discover(logger *zap.Logger, overlay *kademlia.Protocol) {
	ids := overlay.Discover()

	var strList []string
	for _, id := range ids {
		strList = append(strList, fmt.Sprintf(`"%s(%s)"`, id.Address, id.PubKey.String()[:PrintedLength]))
	}

	if len(ids) > 0 {
		logger.Info(fmt.Sprintf("Discovered %d peer(s)", len(ids)),
			zap.String("detail", strings.Join(strList, ", ")),
		)
	}
}

// handle handles and prints out valid messages from peers.
func handleRequest(ctx p2p.HandlerContext) error {
	if !ctx.IsRequest() {
		return nil
	}

	obj, err := ctx.DecodeMessage()
	if err != nil {
		return nil
	}

	msg, ok := obj.(MessageOverP2P)
	if !ok {
		atomic.AddUint32(&receivedMessageFailedOverP2P, 1)
		return nil
	}

	if !msg.IsValid() || len(msg.payLoad) == 0 {
		atomic.AddUint32(&receivedMessageFailedOverP2P, 1)
		return nil
	}

	ReceivedMessageOverP2PChan <- &msg

	bytes := uint32(len(ctx.Data()))
	atomic.AddUint32(&requestHandleOverP2P, 1)
	atomic.AddUint32(&requestHandleByteOverP2P, bytes)

	atomic.AddUint32(&receivedMessageByteOverP2P, bytes)
	atomic.AddUint32(&receivedMessageOverP2P, 1)

	info := ""
	if msg.swapNodeIdsInfo(ctx) {
		info = "[Swap NodeIds Info]"
		atomic.AddUint32(&swapNodeIdsInfoWithPeer, 1)
	}

	ctx.Logger().Debug(fmt.Sprintf("node_service/handle: %s => ", info),
		zap.String("OpCode", msg.OpCodeString()),
		zap.String("PayLoad Size", humanize.Bytes(uint64(len(msg.payLoad)))),
	)

	return nil
}

func handleSend(ctx p2p.HandlerContext) error {
	if ctx.IsRequest() {
		return nil
	}

	obj, err := ctx.DecodeMessage()
	if err != nil {
		atomic.AddUint32(&receivedMessageFailedOverP2P, 1)
		return nil
	}

	msg, ok := obj.(MessageOverP2P)
	if !ok {
		atomic.AddUint32(&receivedMessageFailedOverP2P, 1)
		return nil
	}

	if !msg.IsValid() || len(msg.payLoad) == 0 {
		atomic.AddUint32(&receivedMessageFailedOverP2P, 1)
		return nil
	}

	ReceivedMessageOverP2PChan <- &msg

	atomic.AddUint32(&receivedMessageByteOverP2P, uint32(len(ctx.Data())))
	atomic.AddUint32(&receivedMessageOverP2P, 1)

	ctx.Logger().Debug("node_service/handle send message => ",
		zap.String("OpCode", msg.OpCodeString()),
		zap.String("PayLoad Size", humanize.Bytes(uint64(len(msg.payLoad)))),
	)

	return nil
}

func startProcessReceivedMessageOverP2PJobTask(logger *zap.Logger, b *mqtt.Broker) {
	go func() {
		for msgOverP2P := range ReceivedMessageOverP2PChan {
			err := msgOverP2P.ExecuteTaskAccordingMessageOverP2P(b)
			if err != nil {
				atomic.AddUint32(&executeMessageFailedOverP2P, 1)
				logger.Error("Broker_p2p_module/node_service/StartProcessReceivedMessageOverP2PJobTask : ExecuteTaskAccordingMessageOverP2P error ",
					zap.Error(err),
				)
			}
			atomic.AddUint32(&executeMessageOverP2P, 1)
		}
	}()
}

func startMetricsJobTask(b *mqtt.Broker) {
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			lastMinuteMetricsInfo := fmt.Sprintf(`{"sent":%d,"sent failed":%d,"received":%d,"received failed":%d,"swap node_ids info":%d,"execute":%d,"execute failed":%d,"ids info failed":%d,"topic actions failed":%d,"forward packets failed":%d,"request handle":%d,"sent bytes":%d,"received bytes":%d,"request handle bytes":%d}`,
				sentMessageOverP2P,
				sentMessageFailedOverP2P,
				receivedMessageOverP2P,
				receivedMessageFailedOverP2P,
				swapNodeIdsInfoWithPeer,
				executeMessageOverP2P,
				executeMessageFailedOverP2P,
				nodeIdsInfoFailedOverP2P,
				topicActionsFailedOverP2P,
				forwardPacketsFailedOverP2P,
				requestHandleOverP2P,
				sentMessageByteOverP2P,
				receivedMessageByteOverP2P,
				requestHandleByteOverP2P,
			)

			atomic.AddUint32(&sentMessageOverP2PGrandTotal, atomic.SwapUint32(&sentMessageOverP2P, 0))
			atomic.AddUint32(&sentMessageFailedOverP2PGrandTotal, atomic.SwapUint32(&sentMessageFailedOverP2P, 0))
			atomic.AddUint32(&receivedMessageOverP2PGrandTotal, atomic.SwapUint32(&receivedMessageOverP2P, 0))
			atomic.AddUint32(&receivedMessageFailedOverP2PGrandTotal, atomic.SwapUint32(&receivedMessageFailedOverP2P, 0))
			atomic.AddUint32(&swapNodeIdsInfoWithPeerGrandTotal, atomic.SwapUint32(&swapNodeIdsInfoWithPeer, 0))
			atomic.AddUint32(&executeMessageOverP2PGrandTotal, atomic.SwapUint32(&executeMessageOverP2P, 0))
			atomic.AddUint32(&executeMessageFailedOverP2PGrandTotal, atomic.SwapUint32(&executeMessageFailedOverP2P, 0))
			atomic.AddUint32(&nodeIdsInfoFailedOverP2PGrandTotal, atomic.SwapUint32(&nodeIdsInfoFailedOverP2P, 0))
			atomic.AddUint32(&topicActionsFailedOverP2PGrandTotal, atomic.SwapUint32(&topicActionsFailedOverP2P, 0))
			atomic.AddUint32(&forwardPacketsFailedOverP2PGrandTotal, atomic.SwapUint32(&forwardPacketsFailedOverP2P, 0))
			atomic.AddUint32(&requestHandleOverP2PGrandTotal, atomic.SwapUint32(&requestHandleOverP2P, 0))

			atomic.AddUint32(&sentMessageByteOverP2PGrandTotal, atomic.SwapUint32(&sentMessageByteOverP2P, 0))
			atomic.AddUint32(&receivedMessageByteOverP2PGrandTotal, atomic.SwapUint32(&receivedMessageByteOverP2P, 0))
			atomic.AddUint32(&requestHandleByteOverP2PGrandTotal, atomic.SwapUint32(&requestHandleByteOverP2P, 0))

			grandTotalMetricsInfo := fmt.Sprintf(`{"sent":"%s","sent failed":"%s","received":"%s","received failed":"%s","swap node_ids info":"%s","execute":"%s","execute failed":"%s","ids info failed":"%s","topic actions failed":"%s","forward packets failed":"%s","request handle":"%s","sent bytes":"%s","received bytes":"%s","request handle bytes":"%s"}`,
				humanize.Comma(int64(sentMessageOverP2PGrandTotal)),
				humanize.Comma(int64(sentMessageFailedOverP2PGrandTotal)),
				humanize.Comma(int64(receivedMessageOverP2PGrandTotal)),
				humanize.Comma(int64(receivedMessageFailedOverP2PGrandTotal)),
				humanize.Comma(int64(swapNodeIdsInfoWithPeer)),
				humanize.Comma(int64(executeMessageOverP2PGrandTotal)),
				humanize.Comma(int64(executeMessageFailedOverP2PGrandTotal)),
				humanize.Comma(int64(nodeIdsInfoFailedOverP2PGrandTotal)),
				humanize.Comma(int64(topicActionsFailedOverP2PGrandTotal)),
				humanize.Comma(int64(forwardPacketsFailedOverP2PGrandTotal)),
				humanize.Comma(int64(requestHandleOverP2PGrandTotal)),
				humanize.Comma(int64(sentMessageByteOverP2PGrandTotal)),
				humanize.Comma(int64(receivedMessageByteOverP2PGrandTotal)),
				humanize.Comma(int64(requestHandleByteOverP2PGrandTotal)),
			)

			b.MessageOverP2PMetricsNotification(b.BrokerID().String(), lastMinuteMetricsInfo, grandTotalMetricsInfo)
		}
	}()
}
