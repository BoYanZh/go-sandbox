package container

import (
	"os"
	"syscall"
	"testing"

	"github.com/criyle/go-sandbox/pkg/unixsocket"
)

// fdCount reports the number of open file descriptors for the current process.
func fdCount(t *testing.T) int {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("read /proc/self/fd: %v", err)
	}
	return len(ents)
}

// TestOpenMultiBatchErrorNoFDLeak drives container.Open over two batches where
// the first batch succeeds (returning real FDs) and the second fails. It
// verifies Open returns the partial results so the deferred cleanup closes the
// already-opened files, i.e. no host-side FD leak.
func TestOpenMultiBatchErrorNoFDLeak(t *testing.T) {
	c := &container{
		recvCh: make(chan recvReply, 1),
		sendCh: make(chan sendCmd, 1),
		done:   make(chan struct{}),
	}

	// One file more than a single batch forces exactly two batches.
	total := maxOpenPerBatch + 1
	cmds := make([]OpenCmd, total)
	for i := range cmds {
		cmds[i] = OpenCmd{Path: "/w/f"}
	}

	// Simulated container side: consume sendCh, reply on recvCh.
	go func() {
		// Batch 1: succeed for all maxOpenPerBatch files with real FDs from pipes.
		<-c.sendCh
		fds := make([]int, maxOpenPerBatch)
		for i := range fds {
			var p [2]int
			if err := syscall.Pipe2(p[:], syscall.O_CLOEXEC); err != nil {
				t.Errorf("pipe2: %v", err)
				return
			}
			// keep the read end as the "opened file" fd, close the write end
			fds[i] = p[0]
			syscall.Close(p[1])
		}
		c.recvCh <- recvReply{
			Reply: reply{BatchErrors: make([]string, maxOpenPerBatch)},
			Msg:   unixsocket.Msg{Fds: fds},
		}
		// Batch 2: return a container error so Open fails mid-way.
		<-c.sendCh
		c.recvCh <- recvReply{
			Reply: reply{Error: &errorReply{Msg: "simulated failure"}},
		}
	}()

	before := fdCount(t)
	results, err := c.Open(cmds)
	if err == nil {
		t.Fatal("expected error from second batch, got nil")
	}
	// Open must not have leaked the FDs opened in the first batch: the deferred
	// cleanup should have closed every *os.File in the partial results.
	after := fdCount(t)
	if after > before {
		t.Errorf("FD leak: before=%d after=%d (%d fds leaked)", before, after, after-before)
	}
	// Results may be returned non-nil (partial) but every File must be closed;
	// double-closing is not our concern here, absence of leak is.
	_ = results
}

// TestOpenSingleBatchErrorNoFDLeak covers the single-batch path: a container
// error after some files were opened must still close those files.
func TestOpenSingleBatchErrorNoFDLeak(t *testing.T) {
	c := &container{
		recvCh: make(chan recvReply, 1),
		sendCh: make(chan sendCmd, 1),
		done:   make(chan struct{}),
	}

	cmds := []OpenCmd{{Path: "/w/a"}, {Path: "/w/b"}, {Path: "/w/c"}}

	go func() {
		<-c.sendCh
		// Report 3 BatchErrors but only 1 FD -> triggers the "mismatch between
		// success flags and received FDs" path after consuming one real fd.
		var p [2]int
		if err := syscall.Pipe2(p[:], syscall.O_CLOEXEC); err != nil {
			t.Errorf("pipe2: %v", err)
			return
		}
		syscall.Close(p[1])
		c.recvCh <- recvReply{
			Reply: reply{BatchErrors: []string{"", "", ""}},
			Msg:   unixsocket.Msg{Fds: []int{p[0]}},
		}
	}()

	before := fdCount(t)
	_, err := c.Open(cmds)
	if err == nil {
		t.Fatal("expected mismatch error, got nil")
	}
	after := fdCount(t)
	if after > before {
		t.Errorf("FD leak: before=%d after=%d (%d fds leaked)", before, after, after-before)
	}
}
