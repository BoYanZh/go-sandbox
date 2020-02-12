package daemon

import (
	"fmt"
	"syscall"
	"time"

	"github.com/criyle/go-sandbox/pkg/forkexec"
	"github.com/criyle/go-sandbox/pkg/unixsocket"
	"github.com/criyle/go-sandbox/types"
)

func (c *containerServer) handleExecve(cmd *ExecCmd, msg *unixsocket.Msg) error {
	var (
		files    []uintptr
		execFile uintptr
		cred     *syscall.Credential
	)
	if cmd == nil {
		return c.sendErrorReply("execve: no parameter provided")
	}
	if msg != nil {
		files = intSliceToUintptr(msg.Fds)
		// don't leak fds to child
		closeOnExecFds(msg.Fds)
		// release files after execve
		defer closeFds(msg.Fds)
	}

	// if fexecve, then the first fd must be executable
	if cmd.FdExec {
		if len(files) == 0 {
			return fmt.Errorf("execve: expected fexecve fd")
		}
		execFile = files[0]
		files = files[1:]
	}

	syncFunc := func(pid int) error {
		msg2 := &unixsocket.Msg{
			Cred: &syscall.Ucred{
				Pid: int32(pid),
				Uid: uint32(syscall.Getuid()),
				Gid: uint32(syscall.Getgid()),
			},
		}
		if err2 := c.sendReply(&Reply{}, msg2); err2 != nil {
			return fmt.Errorf("syncFunc: sendReply(%v)", err2)
		}
		cmd2, _, err2 := c.recvCmd()
		if err2 != nil {
			return fmt.Errorf("syncFunc: recvCmd(%v)", err2)
		}
		if cmd2.Cmd == cmdKill {
			return fmt.Errorf("syncFunc: received kill")
		}
		return nil
	}

	if c.Cred {
		cred = &syscall.Credential{
			Uid:         containerUID,
			Gid:         containerGID,
			NoSetGroups: true,
		}
	}

	r := forkexec.Runner{
		Args:       cmd.Argv,
		Env:        cmd.Env,
		ExecFile:   execFile,
		RLimits:    cmd.RLimits,
		Files:      files,
		WorkDir:    "/w",
		NoNewPrivs: true,
		DropCaps:   true,
		SyncFunc:   syncFunc,
		Credential: cred,
	}
	// starts the runner, error is handled same as wait4 to make communication equal
	pid, err := r.Start()

	// done is to signal kill goroutine exits
	killDone := make(chan struct{})
	// waitDone is to signal kill goroutine to collect zombies
	waitDone := make(chan struct{})

	// recv kill
	go func() {
		// signal done
		defer close(killDone)
		// msg must be kill
		c.recvCmd()
		// kill all
		syscall.Kill(-1, syscall.SIGKILL)
		// make sure collect zombie does not consume the exit status
		<-waitDone
		// collect zombies
		for {
			if pid, err := syscall.Wait4(-1, nil, syscall.WNOHANG, nil); err != syscall.EINTR || pid <= 0 {
				break
			}
		}
	}()

	// wait pid if no error encountered for execve
	var wstatus syscall.WaitStatus
	var rusage syscall.Rusage
	if err == nil {
		_, err = syscall.Wait4(pid, &wstatus, 0, &rusage)
	}
	// sync with kill goroutine
	close(waitDone)

	if err != nil {
		c.sendErrorReply("execve: wait4 %v", err)
	} else {
		status := types.StatusNormal
		userTime := time.Duration(rusage.Utime.Nano()) // ns
		userMem := types.Size(rusage.Maxrss << 10)     // bytes
		switch {
		case wstatus.Exited():
			exitStatus := wstatus.ExitStatus()
			c.sendReply(&Reply{
				ExecReply: &ExecReply{
					Status:     status,
					ExitStatus: exitStatus,
					Time:       userTime,
					Memory:     userMem,
				},
			}, nil)

		case wstatus.Signaled():
			switch wstatus.Signal() {
			// kill signal treats as TLE
			case syscall.SIGXCPU, syscall.SIGKILL:
				status = types.StatusTimeLimitExceeded
			case syscall.SIGXFSZ:
				status = types.StatusOutputLimitExceeded
			case syscall.SIGSYS:
				status = types.StatusDisallowedSyscall
			default:
				status = types.StatusSignalled
			}
			c.sendReply(&Reply{
				ExecReply: &ExecReply{
					ExitStatus: int(wstatus.Signal()),
					Status:     status,
					Time:       userTime,
					Memory:     userMem,
				},
			}, nil)

		default:
			c.sendErrorReply("execve: unknown status %v", wstatus)
		}
	}

	// wait for kill msg and reply done for finish
	<-killDone
	return c.sendReply(&Reply{}, nil)
}
