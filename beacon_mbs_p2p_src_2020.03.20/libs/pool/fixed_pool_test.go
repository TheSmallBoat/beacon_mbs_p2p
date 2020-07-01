package pool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var sum int32
var wg sync.WaitGroup

func myFuncSleepMillisecond() {
	atomic.AddInt32(&sum, 1)
	time.Sleep(1 * time.Millisecond)
	wg.Done()
}
func TestFixedWorkerPools(t *testing.T) {
	fwp := NewFixedWorkPool(5)
	assert.EqualValues(t, 5, fwp.maxWorkers)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		fwp.SubmitTask(myFuncSleepMillisecond)
	}

	wg.Wait()
	assert.EqualValues(t, sum, int32(fwp.metrics.numOfTaskSubmitted+fwp.metrics.numOTNilTaskSubmitted))
	assert.EqualValues(t, sum, int32(fwp.metrics.numOfSucceedTaskExecuted+fwp.metrics.numOfFailedTaskExecuted))
}
