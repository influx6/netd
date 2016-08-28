package relay

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/influx6/netd"
	"github.com/influx6/netd/connectors/tcp"
	"github.com/influx6/netd/parser"
	"github.com/influx6/netd/routes"
)

var (
	ctrl        = "\r\n"
	spaceString = []byte(" ")
	endTrace    = []byte("End Trace")
	ctrlLine    = []byte(ctrl)
	newLine     = []byte("\n")

	// message types for different responses
	okMessage   = []byte("+OK\r\n")
	pingMessage = []byte("PING\r\n")
	pongMessage = []byte("P0NG\r\n")
	errMessage  = []byte("+ERR")

	// request message types
	connect  = []byte("CONNECT")
	info     = []byte("INFO")
	sub      = []byte("SUB")
	unsub    = []byte("UNSUB")
	subs     = []byte("SUBS")
	cluster  = []byte("CLUSTER")
	msgBegin = []byte("MSG_PAYLOAD")
	msgEnd   = []byte("MSG_END")

	invalidClusterInfo  = []byte("Invalid Cluster Data, expected {CLUSTER|ADDR|PORT}")
	noResponse          = []byte("Failed to recieve response")
	invalidInfoResponse = []byte("Failed to unmarshal info data")
	negotationFailed    = []byte("Failed to negotiate with new cluster")
	expectedInfoFailed  = []byte("Failed to receive info response for connect")
)

// ClientHandler provides a package-level netd.Provider generator function which can be used to
// create relay client type providers using with the netd.Conn interface.
func ClientHandler(context interface{}, c *tcp.Connection) (netd.Provider, error) {
	c.Config.Log.Log(context, "relay.Handler", "Relay Generating Handler for {%+s}", c.RemoteAddr())

	rl := relay{
		parser:       parser.BlockParser,
		BaseProvider: tcp.NewBaseProvider(c.Router, c),
	}

	go rl.ReadLoop(context)

	c.Config.Log.Log(context, "relay.Handler", "Relay Provider Generated for {%+s}", c.RemoteAddr())
	return &rl, nil
}

// ClusterHandler provides a package-level netd.Provider generator function which can be used to
// create relay client type providers using with the netd.Conn interface.
func ClusterHandler(context interface{}, c *tcp.Connection) (netd.Provider, error) {
	c.Config.Log.Log(context, "relay.Handler", "Relay Generating Handler for {%+s}", c.RemoteAddr())

	rl := relay{
		isCluster:    true,
		parser:       parser.BlockParser,
		BaseProvider: tcp.NewBaseProvider(c.Router, c),
	}

	go rl.ReadLoop(context)

	c.Config.Log.Log(context, "relay.Handler", "Relay Provider Generated for {%+s}", c.RemoteAddr())
	return &rl, nil
}

// relay is a type of netd Handler which typically works like a distributed
// PubSub server, where it allows messages based on matching criterias to be
// matched against clients listening for specific criteria.
// Clusters in relay are simply distinct versions of a relay Handler running
// on another endpoint be it locally or remotely and recieve broadcasts and
// share subscriptions critera lists. This allows clusters on remote hosts
// to share/publish to distributed hosts, more over, it allows clusters that
// maybe dying off to shift connections to another hosts while re-spawning
// themselves.
type relay struct {
	*tcp.BaseProvider
	isCluster    bool
	router       *routes.Subscription
	parser       parser.MessageParser
	providedInfo *netd.BaseInfo
	scratch      bytes.Buffer // scratch buffer for payload.
}

func (rl *relay) negotiateCluster(context interface{}) error {
	rl.Config.Log.Log(context, "negotiateCluster", "Started : Cluster[%s] : Negotiating Begun", rl.Addr)

	if rl.Connection == nil || rl.Connection.Conn == nil {
		return errors.New("Relay underline connection closed")
	}

	if err := rl.SendMessage(context, wrapBlock(connect), true); err != nil {
		rl.Config.Log.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	block := make([]byte, netd.MIN_DATA_WRITE_SIZE)

	rl.Conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	n, err := rl.Conn.Read(block)
	if err != nil {
		rl.Conn.SetReadDeadline(time.Time{})
		rl.Config.Log.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	rl.Conn.SetReadDeadline(time.Time{})

	messages, err := rl.parser.Parse(block[:n])
	if err != nil {
		rl.Config.Log.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	if len(messages) == 0 {
		if err := rl.SendMessage(context, noResponse, true); err != nil {
			rl.Config.Log.Error(context, "negotiateCluster", err, "Completed")
			return err
		}

		err := errors.New("Invalid negotation message received")
		rl.Config.Log.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	infoMessage := messages[0]
	if !bytes.Equal(infoMessage.Command, info) {
		if err := rl.SendMessage(context, expectedInfoFailed, true); err != nil {
			rl.Config.Log.Error(context, "negotiateCluster", err, "Completed")
			return err
		}

		err := errors.New("Invalid connect response received")
		rl.Config.Log.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	infoData := bytes.Join(infoMessage.Data, emptyString)

	var realInfo netd.BaseInfo

	if err := json.Unmarshal(infoData, &realInfo); err != nil {
		if err := rl.SendMessage(context, invalidInfoResponse, true); err != nil {
			rl.Config.Log.Error(context, "negotiateCluster", err, "Completed")
			return err
		}

		rl.Config.Log.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	rl.BaseProvider.MyInfo = realInfo

	if err := rl.SendMessage(context, okMessage, true); err != nil {
		rl.Config.Log.Error(context, "negotiateCluster", err, "Completed")
		return err
	}

	rl.Config.Log.Log(context, "negotiateCluster", "Completed")
	return nil
}

func (rl *relay) ReadLoop(context interface{}) {
	rl.Config.Log.Log(context, "ReadLoop", "Started : Relay Provider for Connection{%+s}  read loop", rl.Addr)

	var isCluster bool

	rl.Lock.Lock()
	isCluster = rl.isCluster
	rl.Lock.Unlock()

	if isCluster {
		if err := rl.negotiateCluster(context); err != nil {
			rl.SendMessage(context, makeErr("Error negotiating with  cluster: %s", err.Error()), true)
			rl.Waiter.Done()
			rl.Close(context)
			return
		}
	}

	rl.Lock.Lock()
	defer rl.Waiter.Done()
	rl.Lock.Unlock()

	block := make([]byte, netd.MIN_DATA_WRITE_SIZE)

	{
	loopRunner:
		for rl.IsRunning() {

			if rl.Conn == nil {
				break loopRunner
			}

			n, err := rl.Conn.Read(block)
			if err != nil {
				go rl.Close(context)
				break loopRunner
			}

			rl.Config.Trace.Trace(context, []byte("--TRACE Started ------------------------------\n"))
			rl.Config.Trace.Trace(context, []byte(fmt.Sprintf("Connection %s\n", rl.Addr)))
			rl.Config.Trace.Trace(context, []byte(fmt.Sprintf("%q", block[:n])))
			rl.Config.Trace.Trace(context, []byte("\n"))
			rl.Config.Trace.Trace(context, []byte("--TRACE Finished --------------------------\n"))

			if err := rl.parse(context, block[:n]); err != nil {
				rl.SendMessage(context, makeErr("Error reading from client: %s", err.Error()), true)
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

	rl.Config.Log.Log(context, "ReadLoop", "Completed  :  Connection{%+s}", rl.Addr)
}

// Parse parses the provided slice of bytes recieved from the relay read loop and using the internal
// parser to retrieve the messages and the appropriate actions to take.
func (rl *relay) parse(context interface{}, data []byte) error {
	rl.Config.Log.Log(context, "parse", "Started  :  Connection [%s] :  Data{%+q}", rl.Addr, data)

	var isCl bool

	rl.Lock.Lock()
	isCl = rl.isCluster
	rl.Lock.Unlock()

	messages, err := rl.parser.Parse(data)
	if err != nil {
		rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection[%s] : Data{%+q}", rl.Addr)
		return err
	}

	rl.Config.Trace.Trace(context, []byte("--TRACE Started ------------------------------\n"))
	rl.Config.Trace.Trace(context, []byte(fmt.Sprintf("%+q\n", messages)))
	rl.Config.Trace.Trace(context, []byte("--TRACE Finished --------------------------\n"))

	for _, message := range messages {
		cmd := bytes.ToUpper(message.Command)
		dataLen := len(message.Data)

		switch {
		case bytes.Equal(cmd, connect):
			if !isCl {
				rl.SendMessage(context, makeErr("Error handling CONNECT for non-cluster: %s", err.Error()), true)
				rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
				return err
			}

			info, err := json.Marshal(rl.Connection.ServerInfo)
			if err != nil {
				rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
				return err
			}

			rl.SendMessage(context, info, true)
		case bytes.Equal(cmd, info):
			info, err := json.Marshal(rl.BaseInfo())
			if err != nil {
				rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
				return err
			}

			rl.SendMessage(context, info, true)
		case bytes.Equal(cmd, cluster):
			if dataLen != 2 {
				err := errors.New("Invalid Cluster Data, expected {CLUSTER|ADDR|PORT}")
				rl.SendMessage(context, invalidClusterInfo, true)
				rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
				return err
			}

			addr := string(message.Data[0])
			port, err := strconv.Atoi(string(message.Data[1]))
			if err != nil {
				rl.SendMessage(context, []byte("Port is not a int: "+err.Error()), true)
				rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
				return err
			}

			if err := rl.Connections.NewClusterFromAddr(context, addr, port); err != nil {
				rl.SendMessage(context, []byte("New Cluster Connect failed: "+err.Error()), true)
				rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
				return err
			}

			return rl.SendMessage(context, okMessage, true)
		case bytes.Equal(cmd, sub):

		case bytes.Equal(cmd, unsub):
		case bytes.Equal(cmd, msgBegin):
		case bytes.Equal(cmd, msgEnd):
		}
	}

	rl.Config.Log.Log(context, "parse", "Completed  :  Connection [%s]", rl.Addr)
	return nil
}
