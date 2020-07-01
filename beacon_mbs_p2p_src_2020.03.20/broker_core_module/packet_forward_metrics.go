package broker_core_module

import (
	"fmt"
	"sync/atomic"

	"github.com/dustin/go-humanize"
)

type PacketForwardMetrics struct {
	numOfCandidate              uint64
	numOfForwarding             uint64
	numOfForwardToMultiBroker   uint64
	numOfForwardParcelOverP2P   uint64
	numOfTopicMatchBrokerFailed uint64
}

func (p *PacketForwardMetrics) increasingNumOfCandidate() {
	atomic.AddUint64(&p.numOfCandidate, 1)
}

func (p *PacketForwardMetrics) increasingNumOfForwarding() {
	atomic.AddUint64(&p.numOfForwarding, 1)
}

func (p *PacketForwardMetrics) increasingNumOfForwardToMultiBroker() {
	atomic.AddUint64(&p.numOfForwardToMultiBroker, 1)
}

func (p *PacketForwardMetrics) increasingNumOfForwardParcelOverP2P() {
	atomic.AddUint64(&p.numOfForwardParcelOverP2P, 1)
}

func (p *PacketForwardMetrics) increasingNumOfTopicMatchBrokerFailed() {
	atomic.AddUint64(&p.numOfTopicMatchBrokerFailed, 1)
}

func (p *PacketForwardMetrics) MetricsInfo() string {
	return fmt.Sprintf(`{"candidate":"%s","forwarding":"%s","forward to multi-brokers":"%s","forward parcel over p2p":"%s","topic match broker_id failed":%d}`,
		humanize.Comma(int64(p.numOfCandidate)),
		humanize.Comma(int64(p.numOfForwarding)),
		humanize.Comma(int64(p.numOfForwardToMultiBroker)),
		humanize.Comma(int64(p.numOfForwardParcelOverP2P)),
		p.numOfTopicMatchBrokerFailed,
	)
}
