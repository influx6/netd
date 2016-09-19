package nitro

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/influx6/netd"
	"github.com/influx6/netd/tcp"
)

//==============================================================================

// Handlers exposes a package level variable which holds provider methods which
// create clients and clusters for netd tcp/udp connections.
var Handlers nitroHandlers

type nitroHandlers struct{}

// NewTCPClient returns new tcp based client providers which handle connections
// for connected clients.
func (nitroHandlers) NewTCPClient(context interface{}, cx *tcp.Connection) (netd.Provider, error) {
	return tcp.NewTCPProvider(false,
		netd.BlockParser,
		&Nitro{Logger: cx.Config.Logger, Trace: cx.Config.Trace},
		cx), nil
}

// NewTCPCluster returns new tcp based client providers which handle connections
// for connected clients.
func (nitroHandlers) NewTCPCluster(context interface{}, cx *tcp.Connection) (netd.Provider, error) {
	return tcp.NewTCPProvider(true,
		netd.BlockParser,
		&Nitro{Logger: cx.Config.Logger, Trace: cx.Config.Trace},
		cx), nil
}

//==============================================================================

var (

	// request message headers
	batchd      = []byte("+BATCHD")
	sub         = []byte("SUB")
	subs        = []byte("SUBS")
	unsub       = []byte("UNSUB")
	pub         = []byte("PUB")
	pingMessage = []byte("PING")
	pongMessage = []byte("P0NG")
	connect     = []byte("CONNECT")
	payload     = []byte("PAYLOAD")
)

// Nitro implements a netd.RequestResponse interface, providing methods to handle
// and manage the behaviour of a provider.
type Nitro struct {
	netd.Logger
	netd.Trace

	Next         netd.Middleware
	payload      bytes.Buffer
	currentRoute []byte
	currentMsgr  []byte
	pn           int64
}

// HandleData handles the processing of incoming data and sending the response
// down the router line.
func (n *Nitro) HandleData(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleData", "Started : Data : %+q", data)

	if len(data) == 0 {
		return nil, true, errors.New("Invalid data received: Empty slice")
	}

	for _, bu := range data {
		var message netd.SubMessage

		if err := json.NewDecoder(bytes.NewReader(bu)).Decode(&message); err != nil {
			n.Error(context, "Nitro.HandleData", err, "Completed")
			return nil, true, err
		}

		source, ok := message.Source.(map[string]interface{})
		if !ok {
			err := errors.New("Invalid Source Value, expected info map")
			n.Error(context, "Nitro.HandleData", err, "Completed")
			return nil, true, err
		}

		clientID := source["client_id"].(string)
		if clientID == cx.Base.ClientID {
			continue
		}

		var sourceInfo netd.BaseInfo
		sourceInfo.Port = int(source["port"].(float64))
		sourceInfo.MaxPayload = int(source["max_payload"].(float64))
		sourceInfo.Addr = source["addr"].(string)
		sourceInfo.IP = source["ip"].(string)
		sourceInfo.Version = source["version"].(string)
		sourceInfo.GoVersion = source["go_version"].(string)
		sourceInfo.ServerID = source["server_id"].(string)
		sourceInfo.ClientID = source["client_id"].(string)
		// sourceInfo.ClusterNode = source["cluster_node"].(bool)

		cx.Router.Handle(context, message.Topic, message.Payload, sourceInfo)
	}

	n.Log(context, "Nitro.HandleData", "Completed")
	return netd.OkMessage, false, nil
}

// HandleUnsubscribe handles all publish for different topics.
func (n *Nitro) HandlePublish(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandlePublish", "Started : Data : %+q", data)

	if len(data) < 2 {
		err := errors.New("Invalid Data received for publishing")
		n.Error(context, "Nitro.HandlePublish", err, "Completed")
		return nil, true, err
	}

	route := data[0]
	head := data[1]

	switch {
	case bytes.Equal(head, netd.BeginMessage):
		n.currentRoute = route
		n.payloadUp()
	default:
		for _, hm := range data[1:] {
			cx.Router.Handle(context, route, hm, cx.Base)
		}
	}

	n.Log(context, "Nitro.HandlePublish", "Completed")
	return netd.OkMessage, false, nil
}

// HandleMsgBegin handles all distribution of messages to clients.
func (n *Nitro) HandleMsgBegin(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleMsgBegin", "Started : Data : %+q", data)

	if n.payloadMode() {
		err := errors.New("Invalid Connection State, already in payload receive state")
		n.Error(context, "Nitro.HandleMsgBegin", err, "Completed")
		return nil, true, err
	}

	n.payloadUp()

	n.currentMsgr = data[0]
	data = data[1:]

	for _, da := range data {
		n.payload.Write(da)
	}

	n.Log(context, "Nitro.HandleMsgBegin", "Completed")
	return netd.OkMessage, false, nil
}

// HandleMsgEnd handles all distribution of messages to clients.
func (n *Nitro) HandleMsgEnd(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleMsgEnd", "Started : Data : %+q", data)

	if !n.payloadMode() {
		err := errors.New("Invalid Connection State, not in payload receive state")
		n.Error(context, "Nitro.HandleMsgEnd", err, "Completed")
		return nil, true, err
	}

	if err := cx.Connections.SendToClients(context, string(n.currentMsgr), n.payload.Bytes(), true); err != nil {
		n.Error(context, "Nitro.HandleMsgEnd", err, "Completed")
		return nil, true, err
	}

	if err := cx.Connections.SendToClusters(context, string(n.currentMsgr), n.payload.Bytes(), true); err != nil {
		n.Error(context, "Nitro.HandleMsgEnd", err, "Completed")
		return nil, true, err
	}

	n.currentMsgr = nil
	n.payload.Reset()
	n.payloadDown()

	n.Log(context, "Nitro.HandleMsgEnd", "Completed")
	return netd.OkMessage, false, nil
}

// HandlePayload handles gathering payload data into a complete sequence of data sets.
func (n *Nitro) HandlePayload(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandlePayload", "Started : Data : %+q", data)

	if len(data) == 0 {
		return nil, true, errors.New("Empty data received")
	}

	if !n.payloadMode() {
		return nil, true, errors.New("Invalid Data collection mode")
	}

	head := data[0]

	switch {
	case bytes.Equal(head, netd.EndMessage):
		n.payloadDown()
		if len(n.currentRoute) > 1 {
			cx.Router.Handle(context, n.currentRoute, n.payload.Bytes(), cx.Base)
			n.currentRoute = nil
			n.payload.Reset()
		}
	default:
		for _, da := range data {
			n.payload.Write(da)
		}

		return nil, false, nil
	}

	n.Log(context, "Nitro.HandlePayload", "Completed")
	return netd.OkMessage, false, nil
}

// HandleConnect handles the response of sending the server info recieved
// for the connect request.
func (n *Nitro) HandleConnect(context interface{}, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleConnect", "Started")

	info, err := json.Marshal(cx.Server)
	if err != nil {
		n.Error(context, "Nitro.HandleConnect", err, "Completed")
		return nil, true, err
	}

	n.Log(context, "Nitro.HandleConnect", "Completed")
	return netd.WrapResponse(netd.RespMessage, info), false, nil
}

// HandleInfo handles the response to info requests.
func (n *Nitro) HandleInfo(context interface{}, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleInfo", "Started")

	info, err := json.Marshal(cx.Base)
	if err != nil {
		n.Error(context, "Nitro.HandleInfo", err, "Completed")
		return nil, true, err
	}

	n.Log(context, "Nitro.HandleInfo", "Completed")
	return netd.WrapResponse(netd.RespMessage, info), false, nil
}

// HandleCluster handles the cluster request, requesting the new cluster provided
// information be processed.
func (n *Nitro) HandleCluster(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleCluster", "Started")

	dataLen := len(data)
	if dataLen != 2 {
		err := errors.New("Invalid Cluster Data, expected {CLUSTER|ADDR|PORT}")
		n.Error(context, "Nitro.HandleCluster", err, "Completed")
		return nil, true, err
	}

	addr := string(data[0])

	port, err := strconv.Atoi(string(data[1]))
	if err != nil {
		err = fmt.Errorf("Port is not a int: " + err.Error())
		n.Error(context, "Nitro.HandleCluster", err, "Completed")
		return nil, true, err
	}

	if err := cx.Clusters.NewCluster(context, addr, port); err != nil {
		n.Error(context, "Nitro.HandleCluster", err, "Completed")
		return nil, true, err
	}

	n.Log(context, "Nitro.HandleCluster", "Completed")
	return netd.OkMessage, false, nil
}

// HandleUnsubscribe handles all subscription to different topics.
/* We expect topics to be in the forms of the following:

   '*' or '/' => to capture all topics in all level
   'alarm.red' => to capture the 'red' sub topic of the 'alarm' topic.
   'alarm.ish^' => to capture topics published which lies under the 'alarm' topic and has a subtopic ending with 'ish'
   'alarm.^ish' => to capture topics published which lies under the 'alarm' topic and has a subtopic beginning with 'ish'
   'alarm.{color:[.+]}' => to capture topics under 'alarm', which matches the regexp in bracket and is aliased by the 'color' name.
   'alarm.ish*' => to capture topics published which lies under the 'alarm' topic and contains 'ish'
   'alarm.*ish' => to capture topics published which lies under the 'alarm' topic and contains 'ish'
*/
func (n *Nitro) HandleSubscribe(context interface{}, subs [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleSubscribe", "Started : Subscribing to [%+q]", subs)

	for _, sub := range subs {
		if err := cx.Router.Register(sub, cx.Subscriber); err != nil {
			n.Error(context, "Nitro.HandleSubscribe", err, "Completed")
			return nil, false, err
		}
	}

	n.Log(context, "Nitro.HandleSubscribe", "Completed")
	return netd.WrapResponse(nil, netd.OkMessage), false, nil
}

// HandleUnsubscribe handles all unsubscription to different topics.
/* We expect topics to be in the forms of the following:

   '*' or '/' => to be removed from the capture all topics in all level
   'alarm.red' => to be removed from the capture 'red' sub topic of the 'alarm' topic.
   'alarm.ish^' => to be removed from the capture topics published which lies under the 'alarm' topic and has a subtopic ending with 'ish'
   'alarm.^ish' => to be removed from the capture topics published which lies under the 'alarm' topic and has a subtopic beginning with 'ish'
   'alarm.{color:[.+]}' => to be removed from the capture topics under 'alarm', which matches the regexp in bracket and is aliased by the 'color' name.
   'alarm.ish*' => to be removed from the capture topics published which lies under the 'alarm' topic and contains 'ish'
   'alarm.*ish' => to be removed from the capture topics published which lies under the 'alarm' topic and contains 'ish'
*/
func (n *Nitro) HandleUnsubscribe(context interface{}, subs [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleUnsubscribe", "Started : Subscribing to [%+q]", subs)

	for _, sub := range subs {
		if err := cx.Router.Unregister(sub, cx.Subscriber); err != nil {
			n.Error(context, "Nitro.HandleUnsubscribe", err, "Completed")
			return nil, false, err
		}
	}

	n.Log(context, "Nitro.HandleUnsubscribe", "Completed")
	return netd.WrapResponse(nil, netd.OkMessage), false, nil
}

// HandleEvents connects to the connection event provider to listening for
// connects and disconnects.
func (n *Nitro) HandleEvents(context interface{}, cx netd.ConnectionEvents) error {
	n.Log(context, "Nitro.HandleEvents", "Started")

	if n.Next != nil {
		n.Log(context, "Nitro.HandleEvents", "Completed")
		return n.Next.HandleEvents(context, cx)
	}

	n.Log(context, "Nitro.HandleEvents", "Completed")
	return nil
}

// HandleFire handles response to be sent when a route fires off with the providers
// fire method. This returns the expected response.
func (n *Nitro) HandleFire(context interface{}, message *netd.SubMessage) ([]byte, error) {
	n.Log(context, "Nitro.HandleFire", "Started")

	var bu bytes.Buffer

	if err := json.NewEncoder(&bu).Encode(message); err != nil {
		n.Error(context, "Nitro.HandleFire", err, "Completed")
		return nil, err
	}

	n.Log(context, "Nitro.HandleFire", "Completed")
	return netd.WrapResponse(netd.DataMessage, bu.Bytes()), nil
}

// HandleMessage handles the call for the different available message handlers
// for different behaviours of the Nitro Packet parser. Delegating each message
// to the appropriate method else passing it to its next available middleware if
// set.
func (n *Nitro) HandleMessage(context interface{}, cx *netd.Connection, message netd.Message) ([]byte, bool, error) {
	switch {
	case bytes.Equal(message.Command, netd.ConnectMessage):
		return n.HandleConnect(context, cx)
	case bytes.Equal(message.Command, netd.InfoMessage):
		return n.HandleInfo(context, cx)
	case bytes.Equal(message.Command, netd.ClusterMessage):
		return n.HandleCluster(context, message.Data, cx)
	case bytes.Equal(message.Command, pub):
		return n.HandlePublish(context, message.Data, cx)
	case bytes.Equal(message.Command, netd.BeginMessage):
		return n.HandleMsgBegin(context, message.Data, cx)
	case bytes.Equal(message.Command, netd.EndMessage):
		return n.HandleMsgEnd(context, message.Data, cx)
	case bytes.Equal(message.Command, netd.DataMessage):
		return n.HandleData(context, message.Data, cx)
	case bytes.Equal(message.Command, sub):
		return n.HandleSubscribe(context, message.Data, cx)
	case bytes.Equal(message.Command, unsub):
		return n.HandleUnsubscribe(context, message.Data, cx)
	case bytes.Equal(message.Command, payload):
		return n.HandlePayload(context, message.Data, cx)
	case bytes.Equal(message.Command, netd.OkMessage):
		return nil, false, nil
	case bytes.Equal(message.Command, netd.RespMessage):
		return nil, false, nil
	case bytes.Equal(message.Command, netd.ErrMessage):
		return nil, false, nil
	default:
		if n.Next != nil {
			return n.Next.Handle(context, message, cx)
		}

		return nil, false, netd.ErrInvalidRequest
	}
}

// Process implements the netd.RequestResponse.Process method which process all
// incoming messages.
func (n *Nitro) Process(context interface{}, cx *netd.Connection, messages ...netd.Message) (bool, error) {
	n.Log(context, "Nitro.Process", "Started : From{Client: %q, Server: %q} :  Messages {%+q}", cx.Base.ClientID, cx.Server.ServerID, messages)

	var responses [][]byte

	msgLen := len(messages)

	var res []byte
	var doClose bool
	var checkErr bool
	var err error

	for _, message := range messages {
		res, doClose, err = n.HandleMessage(context, cx, message)
		if err != nil {
			if msgLen > 1 {
				responses = append(responses, []byte(err.Error()))
				continue
			}

			checkErr = true
			break
		}

		if res == nil {
			continue
		}

		responses = append(responses, res)
	}

	// If we are allowed to check for errors then check and if found, error out.
	if checkErr && err != nil {
		n.Error(context, "Nitro.Process", err, "Completed")
		return doClose, err
	}

	if err := cx.Messager.Send(context, true, responses...); err != nil {
		n.Error(context, "Nitro.Process", err, "Completed")
		return true, err
	}

	if doClose {
		n.Log(context, "Nitro.Process", "Completed")
		return true, nil
	}

	n.Log(context, "Nitro.Process", "Completed")
	return false, nil
}

// payloadMode returns true/false if the nitro parser in in payload mode, where
// data is being gathered.
func (n *Nitro) payloadMode() bool {
	return atomic.LoadInt64(&n.pn) == 1
}

// payloadDown disables payload mode ending data gathering.
func (n *Nitro) payloadDown() {
	atomic.StoreInt64(&n.pn, 0)
}

// payloadUp enables payload mode for gathering incoming data.
func (n *Nitro) payloadUp() {
	atomic.StoreInt64(&n.pn, 1)
}
