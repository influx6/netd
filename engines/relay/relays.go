package relay

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/influx6/netd"
	"github.com/influx6/netd/parser"
	"github.com/influx6/netd/routes"
)

var (
	ctrl        = "\r\n"
	spaceString = []byte(" ")
	emptyString = []byte("")
	endTrace    = []byte("End Trace")
	ctrlLine    = []byte(ctrl)
	newLine     = []byte("\n")

	// message types for different responses
	okMessage   = []byte("+OK\r\n")
	pingMessage = []byte("PING\r\n")
	pongMessage = []byte("P0NG\r\n")
	errMessage  = []byte("+ERR")

	// request message types
	info     = []byte("INFO")
	sub      = []byte("SUB")
	unsub    = []byte("UNSUB")
	listSub  = []byte("LISTSB")
	msgBegin = []byte("MSG_PAYLOAD")
	msgEnd   = []byte("MSG_END")
)

// ClientHandler provides a package-level netd.Provider generator function which can be used to
// create relay client type providers using with the netd.Conn interface.
func ClientHandler(context interface{}, c *netd.Connection) (netd.Provider, error) {
	c.Config.Log.Log(context, "relay.Handler", "Relay Generating Handler for {%+s}", c.RemoteAddr())

	rl := relay{
		parser:       parser.BlockParser,
		BaseProvider: netd.NewBaseProvider(c.Router, c),
	}

	go rl.ReadLoop(context)

	c.Config.Log.Log(context, "relay.Handler", "Relay Provider Generated for {%+s}", c.RemoteAddr())
	return &rl, nil
}

// ClusterHandler provides a package-level netd.Provider generator function which can be used to
// create relay client type providers using with the netd.Conn interface.
func ClusterHandler(context interface{}, c *netd.Connection) (netd.Provider, error) {
	c.Config.Log.Log(context, "relay.Handler", "Relay Generating Handler for {%+s}", c.RemoteAddr())

	rl := relay{
		isCluster:    true,
		parser:       parser.BlockParser,
		BaseProvider: netd.NewBaseProvider(c.Router, c),
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
	*netd.BaseProvider
	isCluster bool
	router    *routes.Subscription
	parser    parser.MessageParser
	scratch   bytes.Buffer // scratch buffer for payload.
}

func (rl *relay) ReadLoop(context interface{}) {
	rl.Config.Log.Log(context, "ReadLoop", "Started : Relay Provider for Connection{%+s}  read loop", rl.RemoteAddr())

	rl.Lock.Lock()
	defer rl.Waiter.Done()
	rl.Lock.Unlock()

	block := make([]byte, netd.MIN_DATA_WRITE_SIZE)

	{
	loopRunner:
		for rl.IsRunning() {

			n, err := rl.Conn.Read(block)
			if err != nil {
				go rl.Close(context)
				break loopRunner
			}

			rl.Config.Trace.Trace(context, []byte("--TRACE Started ------------------------------\n"))
			rl.Config.Trace.Trace(context, []byte(fmt.Sprintf("Connection %s\n", rl.RemoteAddr())))
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

	rl.Config.Log.Log(context, "ReadLoop", "Completed  :  Connection{%+s}  read loop", rl.RemoteAddr())
}

// Parse parses the provided slice of bytes recieved from the relay read loop and using the internal
// parser to retrieve the messages and the appropriate actions to take.
func (rl *relay) parse(context interface{}, data []byte) error {
	rl.Config.Log.Log(context, "parse", "Started  :  Connection [%s] :  Data{%+q}", rl.RemoteAddr(), data)

	messages, err := rl.parser.Parse(data)
	if err != nil {
		rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection[%s] : Data{%+q}", rl.RemoteAddr())
		return err
	}

	rl.Config.Trace.Trace(context, []byte("--TRACE Started ------------------------------\n"))
	rl.Config.Trace.Trace(context, []byte(fmt.Sprintf("%+q\n", messages)))
	rl.Config.Trace.Trace(context, []byte("--TRACE Finished --------------------------\n"))

	for _, message := range messages {
		cmd := bytes.ToUpper(message.Command)

		switch {
		case bytes.Equal(cmd, info):
			info, err := json.Marshal(rl.BaseInfo())
			if err != nil {
				return err
			}

			rl.SendMessage(context, append(info, ctrlLine...), true)
		case bytes.Equal(cmd, sub):

		case bytes.Equal(cmd, unsub):
		case bytes.Equal(cmd, msgBegin):
		case bytes.Equal(cmd, msgEnd):
		}
	}

	rl.Config.Log.Log(context, "parse", "Completed  :  Connection [%s]", rl.RemoteAddr())
	return nil
}
