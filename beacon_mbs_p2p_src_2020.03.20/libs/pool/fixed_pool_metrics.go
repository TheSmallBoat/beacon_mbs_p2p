package pool

import (
	"fmt"
	"sync/atomic"

	"github.com/dustin/go-humanize"
)

type FixedWorkPoolMetrics struct {
	numOfTaskSubmitted              uint64
	numOTNilTaskSubmitted           uint64
	numOfFailedTaskExecuted         uint64
	numOfSucceedTaskExecuted        uint64
	numOfPublishPacketTaskSubmitted uint64
}

func (f *FixedWorkPoolMetrics) IncreasingPublishPacketTaskSubmitted() {
	atomic.AddUint64(&f.numOfPublishPacketTaskSubmitted, 1)
}

func (f *FixedWorkPoolMetrics) increasingTaskSubmitted() {
	atomic.AddUint64(&f.numOfTaskSubmitted, 1)
}

func (f *FixedWorkPoolMetrics) increasingNilTaskSubmitted() {
	atomic.AddUint64(&f.numOTNilTaskSubmitted, 1)
}

func (f *FixedWorkPoolMetrics) increasingFailedTaskExecuted() {
	atomic.AddUint64(&f.numOfFailedTaskExecuted, 1)
}

func (f *FixedWorkPoolMetrics) increasingSucceedTaskExecuted() {
	atomic.AddUint64(&f.numOfSucceedTaskExecuted, 1)
}

func (f *FixedWorkPoolMetrics) MetricsInfo() string {
	return fmt.Sprintf(`{"task submitted (packet with client)":"%s","task submitted nil":%d,"task executed failed":%d,"task executed succeed":"%s","task submitted (publish-packet from broker) ":"%s"}`,
		humanize.Comma(int64(f.numOfTaskSubmitted)),
		f.numOTNilTaskSubmitted,
		f.numOfFailedTaskExecuted,
		humanize.Comma(int64(f.numOfSucceedTaskExecuted)),
		humanize.Comma(int64(f.numOfPublishPacketTaskSubmitted)),
	)
}
