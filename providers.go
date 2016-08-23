package netd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
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

	running bool
	closer  chan struct{}
}

// NewBaseProvider returns a new instance of a BaseProvider.
func NewBaseProvider(conn *Connection) *BaseProvider {
	var bp BaseProvider
	bp.Connection = conn

	bp.Waiter.Add(1)
	bp.running = true
	bp.closer = make(chan struct{}, 0)
	bp.Writer = bufio.NewWriterSize(bp.Conn, MIN_DATA_WRITE_SIZE)

	return &bp
}

// Close ends the loop cycle for the baseProvider.
func (bp *BaseProvider) Close(context interface{}) error {
	bp.Lock.Lock()
	bp.running = false
	bp.Lock.Unlock()

	bp.Waiter.Wait()
	close(bp.closer)

	bp.Lock.Lock()
	bp.Connection = nil
	bp.Lock.Unlock()
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
func (bp *BaseProvider) Fire(context interface{}, params map[string]interface{}, payload interface{}) error {
	var bu bytes.Buffer

	switch payload.(type) {
	case io.Reader:
		reader := payload.(io.Reader)
		if _, err := io.Copy(&bu, reader); err != nil {
			return err
		}
	case bytes.Buffer:
		bu = payload.(bytes.Buffer)
	case bytes.Buffer:
		bu = payload.(*bytes.Buffer)
	case []byte:
		bu.Write(payload.([]byte))
	default:
		if err := json.NewEncoder(&bu).Encode(payload); err != nil {
			return err
		}
	}

	return bp.SendMessage(context, bu.Bytes(), true)
}

// SendMessage sends a message into the provider connection. This exists for
// the outside which wishes to call a write into the connection.
func (bp *BaseProvider) SendMessage(context interface{}, msg []byte, doFlush bool) error {
	if len(msg) > MAX_PAYLOAD_SIZE {
		return fmt.Errorf("Data is above allowed payload size of %d", MAX_PAYLOAD_SIZE)
	}

	var err error
	if bp.Writer != nil && bp.Connection != nil && bp.Connection.Conn != nil {
		var deadlineSet bool

		if bp.Writer.Available() < len(msg) {
			bp.Conn.SetWriteDeadline(time.Now().Add(DEFAULT_FLUSH_DEADLINE))
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

	return err
}

// BaseInfo returns a BaseInfo struct which contains information on the
// connection.
func (bp *BaseProvider) BaseInfo() BaseInfo {
	var info BaseInfo

	bp.Lock.Lock()
	info = bp.Connection.ConnectionInfo
	bp.Lock.Unlock()

	return info
}

// CloseNoify returns a chan which allows notification of a close state of
// the base provider.
func (bp *BaseProvider) CloseNotify() chan struct{} {
	return bp.closer
}

// ReadLoop provides a means of intersecting with the looping mechanism
// for a BaseProvider, its an optional mechanism to provide a callback
// like state of behaviour for the way the loop works.
func (bp *BaseProvider) ReadLoop(context interface{}, loopFn func(*BaseProvider)) {
	bp.Lock.Lock()
	defer bp.Waiter.Done()
	bp.Lock.Unlock()

	{
		for bp.running {
			loopFn(bp)
		}
	}
}
