package pool

const staticChannelSize = 32 //The static channel size for single task queue

type FixedWorkPool struct {
	maxWorkers uint16
	metrics    *FixedWorkPoolMetrics
	taskQueue  []chan func()
}

func NewFixedWorkPool(maxWorkers uint16) *FixedWorkPool {
	// There must be at least one worker.
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	// taskQueue is unbuffered since items are always removed immediately.
	pool := &FixedWorkPool{
		metrics:    new(FixedWorkPoolMetrics),
		maxWorkers: maxWorkers,
		taskQueue:  make([]chan func(), maxWorkers),
	}
	// Start the task dispatcher.
	pool.dispatch()

	return pool
}

func (f *FixedWorkPool) dispatch() {
	for i := uint16(0); i < f.maxWorkers; i++ {
		f.taskQueue[i] = make(chan func(), staticChannelSize)
		f.executeTask(f.taskQueue[i])
	}
}

func (f *FixedWorkPool) getTaskChannelId() uint16 {
	f.metrics.increasingTaskSubmitted()
	return uint16(f.metrics.numOfTaskSubmitted % uint64(f.maxWorkers))
}

func (f *FixedWorkPool) SubmitTask(task func()) {
	if task != nil {
		f.taskQueue[f.getTaskChannelId()] <- task
	} else {
		f.metrics.increasingNilTaskSubmitted()
	}
}

func (f *FixedWorkPool) executeTask(taskChannel chan func()) {
	go func() {
		var task func()
		var ok bool
		for {
			task, ok = <-taskChannel
			if ok && task != nil {
				// Execute the task.
				task()
				f.metrics.increasingSucceedTaskExecuted()
			} else {
				f.metrics.increasingFailedTaskExecuted()
			}
		}
	}()
}

func (f *FixedWorkPool) Metrics() *FixedWorkPoolMetrics {
	return f.metrics
}
