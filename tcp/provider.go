package tcp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/influx6/netd"
)

// TCPProvider creates a base provider structure for use in writing handlers
// for connections.
// When using TCPProvider, it exposes access to a internal mutex, buffered writer
// and waitgroup. This allows you to write your own read loop and ensuring to call
// done on the waitgroup that will have a initial count of 1 added to it and to
// use the writer to write and expand its capacity as you see fit.
type TCPProvider struct {
	*Connection
	lock           sync.Mutex
	writer         *bufio.Writer
	waiter         sync.WaitGroup
	handler        netd.RequestResponse
	parser         netd.MessageParser
	scratchChannel chan struct{}
	scratchDo      sync.Once
	scratchPad     netd.Messages // used to store initial messages to be processed on readloop run.

	addr string

	running   bool
	isCluster bool
	isClosed  bool
	closer    chan struct{}
	// internalcx netd.Connection
}

// NewTCPProvider returns a new instance of a TCPProvider.
func NewTCPProvider(isCluster bool, parser netd.MessageParser, handler netd.RequestResponse, conn *Connection) *TCPProvider {
	var bp TCPProvider
	bp.Connection = conn
	bp.isCluster = isCluster
	bp.handler = handler
	bp.parser = parser

	bp.addr = conn.RemoteAddr().String()
	bp.waiter.Add(1)
	bp.running = true
	bp.closer = make(chan struct{}, 0)
	bp.scratchChannel = make(chan struct{}, 0)
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

// Defer adds a message into the defered scratch pad which will be processed on the
// next request call.
func (bp *TCPProvider) Defer(context interface{}, msgs ...netd.Message) error {
	bp.Config.Log(context, "Defer", "Started : Message : '%+s'", msgs)
	bp.scratchPad = append(bp.scratchPad, msgs...)
	bp.Config.Log(context, "Defer", "Completed")
	return nil
}

// Fire sends the provided payload into the provided write stream.
func (bp *TCPProvider) Fire(context interface{}, msg *netd.SubMessage) error {
	bp.Config.Log(context, "Fire", "Started : Connection[%+s] : Message[%#v]", bp.addr, msg)

	src, ok := msg.Source.(netd.BaseInfo)
	if !ok {
		err := errors.New("Message source should be base info")
		bp.Config.Error(context, "Fire", err, "Completed")
		return err
	}

	bp.Config.Log(context, "Fire", "Info : Publish Source %q : In Server %q", src.ClientID, src.ServerID)
	bu, err := bp.handler.HandleFire(context, msg)
	if err != nil {
		bp.Config.Error(context, "Fire", err, "Completed")
		return err
	}

	if err := bp.SendMessage(context, bu, true); err != nil {
		bp.Config.Error(context, "Fire", err, "Completed")
		return err
	}

	bp.Config.Log(context, "Fire", "Completed")
	return nil
}

//==============================================================================

// Send sends a giving response to the connection. This is used for mainly responding to
// requests recieved through the pipeline.
func (bp *TCPProvider) Send(context interface{}, doFlush bool, msg ...[]byte) error {
	bp.Config.Log(context, "Send", "Started : Connection[%+s]", bp.addr)

	if len(msg) == 0 {
		bp.Config.Log(context, "Send", "Completed")
		return nil
	}

	response := netd.WrapResponse(nil, msg...)
	if err := bp.SendMessage(context, response, doFlush); err != nil {
		bp.Config.Error(context, "Send", err, "Completed")
		return err
	}

	bp.Config.Log(context, "Send", "Completed")
	return nil
}

//==============================================================================

// SendError sends a giving error response to the connection. This is used for mainly responding to
// requests recieved through the pipeline.
func (bp *TCPProvider) SendError(context interface{}, doFlush bool, msg ...error) error {
	bp.Config.Log(context, "SendError", "Started : Connection[%+s]", bp.addr)

	if len(msg) == 0 {
		bp.Config.Log(context, "SendError", "Completed")
		return nil
	}

	var errs [][]byte

	for _, err := range msg {
		errs = append(errs, []byte(err.Error()))
	}

	if err := bp.SendMessage(context, netd.WrapResponse(netd.ErrMessage, errs...), doFlush); err != nil {
		bp.Config.Error(context, "SendError", err, "Completed")
		return err
	}

	bp.Config.Log(context, "SendError", "Completed")
	return nil
}

//==============================================================================

// SendMessage sends a message into the provider connection. This exists for
// the outside which wishes to call a write into the connection.
func (bp *TCPProvider) SendMessage(context interface{}, msg []byte, doFlush bool) error {
	bp.Config.Log(context, "SendMessage", "Started : Connection[%+s] : Data[%q] :  Flush[%t]", bp.addr, msg, doFlush)

	if len(msg) == 0 {
		bp.Config.Log(context, "SendMessage", "Completed")
		return nil
	}

	if len(msg) > netd.MAX_PAYLOAD_SIZE {
		err := fmt.Errorf("Data is above allowed payload size of %d", netd.MAX_PAYLOAD_SIZE)
		bp.Config.Error(context, "SendMessage", err, "Completed")
		return err
	}

	if !bytes.HasSuffix(msg, netd.CTRLINE) {
		msg = append(msg, netd.CTRLINE...)
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
	info = bp.MyInfo
	bp.lock.Unlock()

	return info
}

// CloseNoify returns a chan which allows notification of a close state of
// the base provider.
func (bp *TCPProvider) CloseNotify() chan struct{} {
	return bp.closer
}

func (rl *TCPProvider) readLoop() {
	context := "tcp.TCPProvider"

	rl.Config.Log(context, "ReadLoop", "Started : ReadLoop for Connection{%+s}", rl.addr)

	var isCluster bool
	var sid string
	var cid string
	var cx netd.Connection

	rl.lock.Lock()
	{

		// cache the netd.ClusterMessage status of the provider.
		isCluster = rl.isCluster

		sid = rl.ServerInfo.ServerID
		cid = rl.MyInfo.ClientID

		// initialize the connection fields with the needed information for
		// information processors.
		cx.Messager = rl
		cx.Subscriber = rl
		cx.DeferRequest = rl
		cx.Router = rl.Router
		cx.Parser = rl.parser
		cx.ClusterConnect = rl
		cx.Base = &(rl.MyInfo)
		cx.Server = &(rl.ServerInfo)
		cx.Connections = rl.Connections

		// Initialize the handler for connection events.
		if err := rl.handler.HandleEvents(context, rl.Events); err != nil {
			rl.lock.Unlock()
			rl.Config.Error(context, "ReadLoop", err, "Completed")
			rl.SendError(context, true, err)
			rl.waiter.Done()
			rl.Close(context)
			return
		}
	}
	rl.lock.Unlock()

	if isCluster && rl.ServerInfo.ConnectInitiator {
		realAddr := fmt.Sprintf("%s:%d", rl.ServerInfo.RealAddr, rl.ServerInfo.RealPort)
		identityMsg := netd.WrapResponseBlock(netd.IdentityMessage, []byte(rl.ServerInfo.ServerID), []byte(realAddr))
		clusterReq := netd.WrapResponse(netd.ConnectMessage, []byte(rl.ServerInfo.ServerID))

		if err := rl.Send(context, true, netd.WrapResponse(nil, identityMsg, clusterReq)); err != nil {
			rl.Config.Error(context, "ReadLoop", err, "Completed")
			rl.SendError(context, true, err)
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
		rl.Config.Log(context, "ReadLoop", "Boot Loop  :  Connection{%+s} : Running[%t] : Closed[%t]", rl.addr, rl.IsRunning(), rl.IsClosed())

	loopRunner:
		for rl.IsRunning() && !rl.IsClosed() {

			// If the scratchPad is not empty, hence has pending messages to be
			// process then send signal to pending go-routine listening to the
			// scratchChannel.
			if rl.scratchPad != nil {
				doClose, err := rl.beginScratchProcedure(context, &cx)
				if err != nil {
					rl.SendError(context, true, err)
				}

				// If we are expected to kill the connection after this error then
				// end the loop and close connection.
				if doClose {
					rl.Config.Log(context, "readLoop", "Server[%q] : Client[%q] : Request to end client readloop", sid, cid)
					go rl.Close(context)
					break loopRunner
				}
			}

			n, err := rl.Conn.Read(block)
			if err != nil {
				rl.Config.Error(context, "readLoop", err, "Read Error : Closing Socket :  Server[%q] : Client[%q]`", sid, cid)
				go rl.Close(context)
				break loopRunner
			}

			rl.Config.Trace.Begin(context, []byte("TCPProvider.readloop"))
			rl.Config.Trace.Trace(context, []byte(fmt.Sprintf("Connection %s", rl.addr)))
			rl.Config.Trace.Trace(context, []byte(fmt.Sprintf("%q", block[:n])))
			rl.Config.Trace.End(context, []byte("TCPProvider.readloop"))

			// Parse current block of data.
			messages, err := rl.parser.Parse(block[:n])
			if err != nil {
				rl.SendError(context, true, err)
				go rl.Close(context)
				break loopRunner
			}

			doClose, err := rl.handler.Process(context, &cx, messages...)
			if err != nil {
				rl.SendError(context, true, err)
			}

			// If we are expected to kill the connection after this error then
			// end the loop and close connection.
			if doClose {
				rl.Config.Log(context, "readLoop", "Server[%q] : Client[%q] : Request to end client readloop", sid, cid)
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

func (rl *TCPProvider) beginScratchProcedure(context interface{}, cx *netd.Connection) (bool, error) {
	rl.Config.Log(context, "beginScratchProcedure", "Started : Addr[%q] : Scratch Messages[%+q]", rl.addr, rl.scratchPad)

	doClose, err := rl.handler.Process(context, cx, rl.scratchPad...)
	if err != nil {
		rl.Config.Error(context, "beginScratchProcedure", err, "Completed")
		rl.SendError(context, true, err)
		rl.Close(context)
		rl.scratchPad = nil
		return doClose, err
	}

	rl.scratchPad = nil
	rl.Config.Log(context, "beginScratchProcedure", "Completed")
	return doClose, nil
}
