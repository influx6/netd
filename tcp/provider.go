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
	"github.com/influx6/netd/parser"
)

var (
	ctrl        = "\r\n"
	allSubs     = []byte("*")
	ctrlLine    = []byte(ctrl)
	lineBreak   = []byte("|")
	spaceString = []byte(" ")
	emptyString = []byte("")
	newLine     = []byte("\n")
	endTrace    = []byte("End Trace")

	// message types for different responses
	errMessage  = []byte("+ERR")
	respMessage = []byte("+RESP")

	// request message types
	sub         = []byte("SUB")
	info        = []byte("INFO")
	subs        = []byte("SUBS")
	okMessage   = []byte("OK")
	unsub       = []byte("UNSUB")
	cluster     = []byte("CLUSTER")
	pingMessage = []byte("PING")
	pongMessage = []byte("P0NG")
	connect     = []byte("CONNECT")
	msgEnd      = []byte("MSG_END")
	msgBegin    = []byte("MSG_PAYLOAD")

	noResponse          = errors.New("Failed to recieve response")
	invalidInfoResponse = errors.New("Failed to unmarshal info data")
	negotationFailed    = errors.New("Failed to negotiate with new cluster")
	expectedInfoFailed  = errors.New("Failed to receive info response for connect")
	invalidClusterInfo  = errors.New("Invalid Cluster Data, expected {CLUSTER|ADDR|PORT}")
)

// TCPProvider creates a base provider structure for use in writing handlers
// for connections.
// When using TCPProvider, it exposes access to a internal mutex, buffered writer
// and waitgroup. This allows you to write your own read loop and ensuring to call
// done on the waitgroup that will have a initial count of 1 added to it and to
// use the writer to write and expand its capacity as you see fit.
type TCPProvider struct {
	*Connection
	lock         sync.Mutex
	writer       *bufio.Writer
	scratch      bytes.Buffer
	waiter       sync.WaitGroup
	providedInfo *netd.BaseInfo
	handler      netd.RequestResponse
	parser       parser.MessageParser

	addr string

	running   bool
	isCluster bool
	isClosed  bool
	closer    chan struct{}
}

// NewTCPProvider returns a new instance of a TCPProvider.
func NewTCPProvider(isCluster bool, parser parser.MessageParser, handler netd.RequestResponse, conn *Connection) *TCPProvider {
	var bp TCPProvider
	bp.Connection = conn
	bp.isCluster = isCluster
	bp.handler = handler
	bp.parser = parser

	bp.addr = conn.RemoteAddr().String()
	bp.waiter.Add(1)
	bp.running = true
	bp.closer = make(chan struct{}, 0)
	bp.writer = bufio.NewWriterSize(bp.Conn, netd.MIN_DATA_WRITE_SIZE)

	go bp.readLoop()

	return &bp
}

// Close ends the loop cycle for the baseProvider.
func (bp *TCPProvider) Close(context interface{}) error {
	bp.Config.Log(context, "Close", "Started : Connection[%+s] ", bp.addr)

	bp.lock.Lock()

	if bp.isClosed {
		bp.lock.Unlock()
		err := errors.New("Already closed")
		bp.Config.Error(context, "Close", err, "Completed ")
		return err
	}

	bp.running = false
	bp.lock.Unlock()

	bp.waiter.Wait()
	close(bp.closer)

	bp.lock.Lock()

	if err := bp.Connection.Close(); err != nil {
		bp.isClosed = true
		bp.lock.Unlock()
		bp.Config.Error(context, "Close", err, "Completed ")
		return err
	}

	bp.isClosed = true
	bp.lock.Unlock()

	bp.Config.Log(context, "Close", "Completed ")
	return nil
}

func (bp *TCPProvider) IsClosed() bool {
	bp.lock.Lock()
	state := bp.isClosed
	bp.lock.Unlock()
	return state
}

// IsRunning returns true/false if the base provider is still running.
func (bp *TCPProvider) IsRunning() bool {
	var done bool

	bp.lock.Lock()
	done = bp.running
	bp.lock.Unlock()

	return done
}

// Fire sends the provided payload into the provided write stream.
func (bp *TCPProvider) Fire(context interface{}, params map[string]string, payload interface{}) error {
	bp.Config.Log(context, "Fire", "Started : Connection[%+s] : Paral[%#v] :  Payload[%#v]", bp.addr, params, payload)

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

	bp.Config.Log(context, "Fire", "Completed")
	return bp.SendMessage(context, bu.Bytes(), true)
}

//==============================================================================

// SendResponse sends a giving response to the connection. This is used for mainly responding to
// requests recieved through the pipeline.
func (bp *TCPProvider) SendRequest(context interface{}, doFlush bool, msg ...[][]byte) error {
	response := parser.WrapResponses(nil, msg...)
	return bp.SendMessage(context, response, doFlush)
}

//==============================================================================

var respHeader = []byte("+RESP")

// SendResponse sends a giving response to the connection. This is used for mainly responding to
// requests recieved through the pipeline.
func (bp *TCPProvider) SendResponse(context interface{}, doFlush bool, msg ...[][]byte) error {
	response := parser.WrapResponses(respHeader, msg...)
	return bp.SendMessage(context, response, doFlush)
}

//==============================================================================

var errorHeader = []byte("+ERR")

// SendError sends a giving error response to the connection. This is used for mainly responding to
// requests recieved through the pipeline.
func (bp *TCPProvider) SendError(context interface{}, doFlush bool, msg ...error) error {
	bp.Config.Log(context, "SendResponse", "Started : Connection[%+s]", bp.addr)

	var errs [][][]byte

	for _, err := range msg {
		errbs := []byte(err.Error())
		errs = append(errs, [][]byte{errbs})
	}

	if err := bp.SendMessage(context, parser.WrapResponses(errorHeader, errs...), doFlush); err != nil {
		bp.Config.Error(context, "SendResponse", err, "Completed")
		return err
	}

	bp.Config.Log(context, "SendResponse", "Completed")
	return nil
}

//==============================================================================

// SendMessage sends a message into the provider connection. This exists for
// the outside which wishes to call a write into the connection.
func (bp *TCPProvider) SendMessage(context interface{}, msg []byte, doFlush bool) error {
	bp.Config.Log(context, "SendMessage", "Started : Connection[%+s] : Data[%q] :  Flush[%t]", bp.addr, msg, doFlush)

	if len(msg) > netd.MAX_PAYLOAD_SIZE {
		err := fmt.Errorf("Data is above allowed payload size of %d", netd.MAX_PAYLOAD_SIZE)
		bp.Config.Error(context, "SendMessage", err, "Completed")
		return err
	}

	if !bytes.HasSuffix(msg, ctrlLine) {
		msg = append(msg, ctrlLine...)
	}

	var err error
	if bp.writer != nil && !bp.IsClosed() {
		var deadlineSet bool

		if bp.writer.Available() < len(msg) {
			bp.Conn.SetWriteDeadline(time.Now().Add(netd.DEFAULT_FLUSH_DEADLINE))
			deadlineSet = true
		}

		_, err = bp.writer.Write(msg)
		if err == nil && doFlush {
			err = bp.writer.Flush()
		}

		if deadlineSet {
			bp.Conn.SetWriteDeadline(time.Time{})
		}
	}

	if err != nil {
		bp.Config.Error(context, "SendMessage", err, "Completed")
		return err
	}

	bp.Config.Log(context, "SendMessage", "Completed")
	return nil
}

// BaseInfo returns a BaseInfo struct which contains information on the
// connection.
func (bp *TCPProvider) BaseInfo() netd.BaseInfo {
	var info netd.BaseInfo

	bp.lock.Lock()
	info = bp.Connection.MyInfo
	bp.lock.Unlock()

	return info
}

// CloseNoify returns a chan which allows notification of a close state of
// the base provider.
func (bp *TCPProvider) CloseNotify() chan struct{} {
	return bp.closer
}

func (rl *TCPProvider) negotiateCluster(context interface{}) error {
	rl.Config.Log(context, "negotiateCluster", "Started : Cluster[%s] : Negotiating Begun", rl.addr)

	if rl.IsClosed() {
		return errors.New("Provider underline connection closed")
	}

	if err := rl.SendRequest(context, true, [][]byte{connect}); err != nil {
		rl.Config.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	block := make([]byte, netd.MIN_DATA_WRITE_SIZE)

	rl.Conn.SetReadDeadline(time.Now().Add(netd.DEFAULT_CLUSTER_NEGOTIATION_TIMEOUT))

	n, err := rl.Conn.Read(block)
	if err != nil {
		rl.Conn.SetReadDeadline(time.Time{})
		rl.Config.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	rl.Conn.SetReadDeadline(time.Time{})

	messages, err := rl.parser.Parse(block[:n])
	if err != nil {
		rl.Config.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	if len(messages) == 0 {
		if err := rl.SendError(context, true, noResponse); err != nil {
			rl.Config.Error(context, "negotiateCluster", err, "Completed")
			return err
		}

		err := errors.New("Invalid negotation message received")
		rl.Config.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	infoMessage := messages[0]
	if !bytes.Equal(infoMessage.Command, respMessage) {
		if err := rl.SendError(context, true, expectedInfoFailed); err != nil {
			rl.Config.Error(context, "negotiateCluster", err, "Completed")
			return err
		}

		err := errors.New("Invalid connect response received")
		rl.Config.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	infoData := bytes.Join(infoMessage.Data, emptyString)

	var realInfo netd.BaseInfo

	if err := json.Unmarshal(infoData, &realInfo); err != nil {
		if err := rl.SendError(context, true, invalidInfoResponse); err != nil {
			rl.Config.Error(context, "negotiateCluster", err, "Completed")
			return err
		}

		rl.Config.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	rl.MyInfo = realInfo

	if err := rl.SendResponse(context, true, [][]byte{okMessage}); err != nil {
		rl.Config.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	rl.Config.Log(context, "negotiateCluster", "Completed")
	return nil
}

func (rl *TCPProvider) readLoop() {
	context := "tcp.TCPProvider"
	rl.Config.Log(context, "ReadLoop", "Started : Provider Provider for Connection{%+s}  read loop", rl.addr)

	var isCluster bool

	rl.lock.Lock()
	isCluster = rl.isCluster
	rl.lock.Unlock()

	if isCluster && rl.ServerInfo.ConnectInitiator {
		if err := rl.negotiateCluster(context); err != nil {
			rl.SendError(context, true, fmt.Errorf("Error negotiating with  cluster: %s", err.Error()))
			rl.waiter.Done()
			rl.Close(context)
			return
		}
	}

	rl.lock.Lock()
	defer rl.waiter.Done()
	rl.lock.Unlock()

	block := make([]byte, netd.MIN_DATA_WRITE_SIZE)

	{
	loopRunner:
		for rl.IsRunning() && !rl.IsClosed() {

			n, err := rl.Conn.Read(block)
			if err != nil {
				go rl.Close(context)
				break loopRunner
			}

			var trace [][]byte
			trace = append(trace, []byte("--TRACE Started ------------------------------\n"))
			trace = append(trace, []byte(fmt.Sprintf("Connection %s\n", rl.addr)))
			trace = append(trace, []byte(fmt.Sprintf("%q", block[:n])))
			trace = append(trace, []byte("\n"))
			trace = append(trace, []byte("--TRACE Finished --------------------------\n"))
			rl.Config.Trace.Trace(context, bytes.Join(trace, emptyString))

			if err := rl.handler.Process(context, block[:n], rl, rl.Router); err != nil {
				rl.SendError(context, true, fmt.Errorf("Error reading from client: %s", err.Error()))
				go rl.Close(context)
				break loopRunner
			}

			if n == len(block) && len(block) < netd.MAX_DATA_WRITE_SIZE {
				block = make([]byte, len(block)*2)
			}

			if n < len(block)/2 && len(block) > netd.MIN_DATA_WRITE_SIZE {
				block = make([]byte, len(block)/2)
			}
		}
	}

	rl.Config.Log(context, "ReadLoop", "Completed  :  Connection{%+s}", rl.addr)
}

func toJSON(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func stripCTRL(msg []byte) []byte {
	return bytes.TrimSuffix(msg, ctrlLine)
}
