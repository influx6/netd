package tcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/influx6/netd"
)

// BaseProvider creates a base provider structure for use in writing handlers
// for connections.
// When using BaseProvider, it exposes access to a internal mutex, buffered writer
// and waitgroup. This allows you to write your own read loop and ensuring to call
// done on the waitgroup that will have a initial count of 1 added to it and to
// use the writer to write and expand its capacity as you see fit.
type BaseProvider struct {
	*Connection
	Lock   sync.Mutex
	Waiter sync.WaitGroup
	Writer *bufio.Writer
	Router netd.Router
	Addr   string

	running bool
	closer  chan struct{}
}

// NewBaseProvider returns a new instance of a BaseProvider.
func NewBaseProvider(router netd.Router, conn *Connection) *BaseProvider {
	var bp BaseProvider
	bp.Connection = conn
	bp.Router = router

	bp.Addr = conn.RemoteAddr().String()
	bp.Waiter.Add(1)
	bp.running = true
	bp.closer = make(chan struct{}, 0)
	bp.Writer = bufio.NewWriterSize(bp.Conn, netd.MIN_DATA_WRITE_SIZE)

	return &bp
}

// Close ends the loop cycle for the baseProvider.
func (bp *BaseProvider) Close(context interface{}) error {
	bp.Config.Log.Log(context, "Close", "Started : Connection[%+s] ", bp.Addr)

	bp.Lock.Lock()

	if bp.Connection == nil || bp.Connection.Conn == nil {
		bp.Lock.Unlock()
		err := errors.New("Already closed")
		bp.Config.Log.Error(context, "Close", err, "Completed ")
		return err
	}

	bp.running = false
	bp.Lock.Unlock()

	bp.Waiter.Wait()
	close(bp.closer)

	bp.Lock.Lock()

	if err := bp.Connection.Close(); err != nil {
		bp.Connection.Conn = nil
		bp.Lock.Unlock()
		bp.Config.Log.Error(context, "Close", err, "Completed ")
		return err
	}

	bp.Connection.Conn = nil
	bp.Lock.Unlock()

	bp.Config.Log.Log(context, "Close", "Completed ")
	return nil
}

// IsRunning returns true/false if the base provider is still running.
func (bp *BaseProvider) IsRunning() bool {
	var done bool

	bp.Lock.Lock()
	done = bp.running
	bp.Lock.Unlock()

	return done
}

// Fire sends the provided payload into the provided write stream.
func (bp *BaseProvider) Fire(context interface{}, params map[string]string, payload interface{}) error {
	bp.Config.Log.Log(context, "Fire", "Started : Connection[%+s] : Paral[%#v] :  Payload[%#v]", bp.Addr, params, payload)

	var bu bytes.Buffer

	switch payload.(type) {
	case io.Reader:
		reader := payload.(io.Reader)
		if _, err := io.Copy(&bu, reader); err != nil {
			return err
		}
	case bytes.Buffer:
		bu = payload.(bytes.Buffer)
	case *bytes.Buffer:
		bu = *(payload.(*bytes.Buffer))
	case []byte:
		bu.Write(payload.([]byte))
	default:
		if err := json.NewEncoder(&bu).Encode(payload); err != nil {
			return err
		}
	}

	bp.Config.Log.Log(context, "Fire", "Completed")
	return bp.SendMessage(context, bu.Bytes(), true)
}

// SendMessage sends a message into the provider connection. This exists for
// the outside which wishes to call a write into the connection.
func (bp *BaseProvider) SendMessage(context interface{}, msg []byte, doFlush bool) error {
	bp.Config.Log.Log(context, "SendMessage", "Started : Connection[%+s] : Data[%s] :  Flush[%t]", bp.Addr, msg, doFlush)

	if len(msg) > netd.MAX_PAYLOAD_SIZE {
		err := fmt.Errorf("Data is above allowed payload size of %d", netd.MAX_PAYLOAD_SIZE)
		bp.Config.Log.Error(context, "SendMessage", err, "Completed")
		return err
	}

	if !bytes.HasSuffix(msg, ctrlLine) {
		msg = append(msg, ctrlLine...)
	}

	var err error
	if bp.Writer != nil && bp.Connection != nil && bp.Connection.Conn != nil {
		var deadlineSet bool

		if bp.Writer.Available() < len(msg) {
			bp.Conn.SetWriteDeadline(time.Now().Add(netd.DEFAULT_FLUSH_DEADLINE))
			deadlineSet = true
		}

		_, err = bp.Writer.Write(msg)
		if err == nil && doFlush {
			err = bp.Writer.Flush()
		}

		if deadlineSet {
			bp.Conn.SetWriteDeadline(time.Time{})
		}
	}

	if err != nil {
		bp.Config.Log.Error(context, "SendMessage", err, "Completed")
		return err
	}

	bp.Config.Log.Log(context, "SendMessage", "Completed")
	return nil
}

// BaseInfo returns a BaseInfo struct which contains information on the
// connection.
func (bp *BaseProvider) BaseInfo() netd.BaseInfo {
	var info netd.BaseInfo

	bp.Lock.Lock()
	info = bp.Connection.MyInfo
	bp.Lock.Unlock()

	return info
}

// CloseNoify returns a chan which allows notification of a close state of
// the base provider.
func (bp *BaseProvider) CloseNotify() chan struct{} {
	return bp.closer
}
