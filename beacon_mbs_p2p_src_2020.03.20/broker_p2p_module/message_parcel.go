package broker_p2p_module

import (
	"sync/atomic"

	p2p "awesomeProject/beacon/p2p_network/core_module"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

var PendingMessageParcelChan = make(chan *PendingMessageParcel, 64)

type PendingMessageParcel struct {
	targetNodeIdAddr string
	messageOverP2P   *MessageOverP2P
}

func NewPendingMessageParcel(targetAddr string, msgOverP2P *MessageOverP2P) *PendingMessageParcel {
	if len(targetAddr) < 1 || msgOverP2P == nil {
		return nil
	}
	return &PendingMessageParcel{targetNodeIdAddr: targetAddr, messageOverP2P: msgOverP2P}
}

func (p *PendingMessageParcel) Pending() {
	PendingMessageParcelChan <- p
}

// will be run in p2p_service/ServiceWithFlag
func startProcessPendingMessageParcelJobTask(logger *zap.Logger, node *p2p.Node) {
	go func() {
		for msgParcel := range PendingMessageParcelChan {
			msgOverP2P := msgParcel.messageOverP2P
			err := msgOverP2P.sendMessageOverP2PToTargetNode(node, msgParcel.targetNodeIdAddr)
			if err != nil {
				atomic.AddUint32(&sentMessageFailedOverP2P, 1)

				logger.Error("Failed to send message ... ",
					zap.Error(err),
					zap.String("Address", msgParcel.targetNodeIdAddr),
					zap.String("Payload Size", humanize.Bytes(uint64(msgOverP2P.Size()))),
				)
			}
			atomic.AddUint32(&sentMessageOverP2P, 1)
			atomic.AddUint32(&sentMessageByteOverP2P, uint32(msgOverP2P.Size()))
		}
	}()
}
