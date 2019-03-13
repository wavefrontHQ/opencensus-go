package wavefront

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultIdleTime = 5 * time.Second
)

const (
	MetricSender int = iota
	HistoSender
	SpanSender
	MaxSenders
)

type Worker struct {
	wg      *sync.WaitGroup
	c       chan SendCmd
	dropped uint64
	active  int32
}

func NewWorker(chanSize int32, wg *sync.WaitGroup) *Worker {
	return &Worker{
		c:  make(chan SendCmd, chanSize),
		wg: wg,
	}
}

func (w *Worker) Work() {
	defer w.wg.Done()
	var T = time.NewTimer(DefaultIdleTime)

LOOP:
	for {
		select {
		case cmd := <-w.c:
			cmd()
			continue
		default:
		}
		if !T.Stop() {
			<-T.C
		}
		T.Reset(DefaultIdleTime)
		select {
		case cmd := <-w.c:
			cmd()
		case <-T.C:
			break LOOP
		}
	}
	atomic.StoreInt32(&w.active, 0)
	for atomic.LoadInt32(&w.active) == 0 { // Drain if no other worker
		select {
		case cmd := <-w.c:
			cmd()
		default:
			return
		}
	}
}

func (w *Worker) Queue(cmd SendCmd) {
	select {
	case w.c <- cmd:
		if atomic.CompareAndSwapInt32(&w.active, 0, 1) == true {
			w.wg.Add(1)
			go w.Work()
		}
	default:
		atomic.AddUint64(&w.dropped, 1)
	}
}

func (w *Worker) DroppedCmds() uint64 {
	return atomic.LoadUint64(&w.dropped)
}
