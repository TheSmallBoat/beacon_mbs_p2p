package main

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	p2p "awesomeProject/beacon/p2p_network/core_module"

	"awesomeProject/beacon/p2p_network/libs/cryptographic"

	"github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

type messageOverP2P struct {
	contents string
}

func (m messageOverP2P) Marshal() []byte {
	return []byte(m.contents)
}

func unmarshalMessageOverP2P(buf []byte) (messageOverP2P, error) {
	return messageOverP2P{contents: strings.ToValidUTF8(string(buf), "")}, nil
}

// Send MessageOverP2P to target node.
func (m *messageOverP2P) SendMessageOverP2PToTargetNode(node *p2p.Node, targetID cryptographic.ID) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err := node.SendMessage(ctx, targetID.Address, *m)
	cancel()

	if err != nil {
		atomic.AddUint32(&sentMessageFailedOverP2P, 1)

		node.Logger().Error("Failed to send message ... ",
			zap.Error(err),
			zap.String("Address", targetID.Address),
			zap.String("Public Key", targetID.PubKey.String()[:PrintedLength]),
			zap.String("Content Size", humanize.Bytes(uint64(1+len(m.contents)))),
		)
	}
	atomic.AddUint32(&sentMessageOverP2P, 1)
	return err
}
