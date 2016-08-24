package engines

import (
	"bytes"
	"encoding/json"

	"github.com/influx6/netd"
	"github.com/influx6/netd/parser"
	"github.com/influx6/netd/routes"
)

// Relay defines a package level handler for accessing the relay engine API.
var Relay relayCtrl

type relayCtrl struct{}

// Relays defines a global function which
func (relayCtrl) Relays(context interface{}, router netd.Router, c *netd.Connection) (netd.Provider, error) {
	rl := relay{
		parser:       parser.BlockParser,
		BaseProvider: netd.NewBaseProvider(router, c),
	}

	go rl.ReadLoop(context)
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
	router  *routes.Subscription
	parser  parser.MessageParser
	scratch bytes.Buffer // scratch buffer for payload.
}

func (rl *relay) ReadLoop(context interface{}) {
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
}

// Parse parses the provided slice of bytes recieved from the relay read loop and using the internal
// parser to retrieve the messages and the appropriate actions to take.
func (rl *relay) parse(context interface{}, data []byte) error {
	messages, err := rl.parser.Parse(data)
	if err != nil {
		return err
	}

	for _, message := range messages {
		cmd := bytes.ToUpper(message.Command)

		switch {
		case bytes.Equal(cmd, infoMessage):
			info, err := json.Marshal(rl.BaseInfo())
			if err != nil {
				return err
			}

			rl.SendMessage(context, info, true)
		case bytes.Equal(cmd, sub):

		case bytes.Equal(cmd, unsub):
		case bytes.Equal(cmd, msgBegin):
		case bytes.Equal(cmd, msgEnd):
		}
	}

	return nil
}
