package nitro

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
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
		&Nitro{
			Logger: cx.Config.Logger,
			Trace:  cx.Config.Trace,
			rcs:    make(map[string]bool),
		},
		cx), nil
}

// NewTCPCluster returns new tcp based client providers which handle connections
// for connected clients.
func (nitroHandlers) NewTCPCluster(context interface{}, cx *tcp.Connection) (netd.Provider, error) {
	return tcp.NewTCPProvider(true,
		netd.BlockParser,
		&Nitro{
			Logger: cx.Config.Logger,
			Trace:  cx.Config.Trace,
			rcs:    make(map[string]bool),
		},
		cx), nil
}

//==============================================================================

var (
	colon = []byte(":")

	// request message headers
	batchd      = []byte("+BATCHD")
	sub         = []byte("SUB")
	subs        = []byte("SUBS")
	listtopics  = []byte("LISTTOPICS")
	topics      = []byte("TOPICS")
	unsub       = []byte("UNSUB")
	pub         = []byte("PUB")
	clusterpub  = []byte("_CPUB")
	exit        = []byte("EXIT")
	pingMessage = []byte("PING")
	pongMessage = []byte("P0NG")
	connect     = []byte("CONNECT")
)

// Nitro implements a netd.RequestResponse interface, providing methods to handle
// and manage the behaviour of a provider.
type Nitro struct {
	netd.Logger
	netd.Trace

	Next         netd.Middleware
	payload      bytes.Buffer
	currentRoute []byte

	rcs map[string]bool
	pn  int64
}

// HandleIdentity handles the identity transfer request received over a cluster connection.
func (n *Nitro) HandleIdentity(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleIdentity", "Started : Data : %+q", data)

	if len(data) < 2 {
		err := errors.New("Received invalid identity data: Slice less than 2 in length")
		n.Error(context, "Nitro.HandleIdentity", err, "Completed")
		return nil, true, netd.ErrExistingCluster
	}

	clusterID := string(data[0])
	addr, port, err := net.SplitHostPort(string(data[1]))
	if err != nil {
		n.Error(context, "Nitro.HandleIdentity", err, "Completed")
		return nil, true, err
	}

	iport, err := strconv.Atoi(port)
	if err != nil {
		n.Error(context, "Nitro.HandleIdentity", err, "Completed")
		return nil, true, err
	}

	clusters := cx.Clusters(context)
	if _, err := clusters.HasAddr(addr, iport); err == nil {
		return nil, true, errors.New("Cluster Already Exists")
	}

	cx.Base.ServerID = clusterID
	cx.Base.RealAddr = addr
	cx.Base.RealPort = iport

	n.Log(context, "Nitro.HandleIdentity", "Completed")
	return netd.OkMessage, false, nil
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
		sourceInfo.FromMap(source)

		cx.Router.Handle(context, message.Topic, message.Payload, sourceInfo)
	}

	n.Log(context, "Nitro.HandleData", "Completed")
	return netd.OkMessage, false, nil
}

// HandlePublish handles all publish for different topics.
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
	case bytes.Equal(head, netd.EndMessage):
		if !n.payloadMode() {
			err := errors.New("Invalid Publish payload mode state")
			n.Error(context, "Nitro.HandleClusterPublish", err, "Completed")
			return nil, true, err
		}

		n.payloadDown()
		croute := n.currentRoute
		pmb := n.payload.Bytes()

		n.currentRoute = nil
		n.payload.Reset()

		cx.Router.Handle(context, croute, pmb, *cx.Base)

		if err := cx.SendToClusters(context, cx.Base.ClientID, netd.WrapResponseBlock(clusterpub, data...), true); err != nil {
			n.Error(context, "Nitro.HandlePublish", err, "Completed")
			return nil, true, err
		}

	case bytes.Equal(head, netd.BeginMessage):
		n.currentRoute = route
		n.payloadUp()

		if err := cx.SendToClusters(context, cx.Base.ClientID, netd.WrapResponseBlock(clusterpub, data...), true); err != nil {
			n.Error(context, "Nitro.HandlePublish", err, "Completed")
			return nil, true, err
		}

	default:
		for _, hm := range data[1:] {
			n.Log(context, "Nitro.HandlePublish", "Routing : Topic[%+s] : Message[%+s]", route, hm)
			cx.Router.Handle(context, route, hm, *cx.Base)
		}

		if err := cx.SendToClusters(context, cx.Base.ClientID, netd.WrapResponseBlock(clusterpub, data...), true); err != nil {
			n.Error(context, "Nitro.HandlePublish", err, "Completed")
			return nil, true, err
		}
	}

	n.Log(context, "Nitro.HandlePublish", "Completed")
	return netd.OkMessage, false, nil
}

// HandleClusterPublish handles all publish for different topics.
func (n *Nitro) HandleClusterPublish(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleClusterPublish", "Started : Data : %+q", data)

	if len(data) < 2 {
		err := errors.New("Invalid Data received for publishing")
		n.Error(context, "Nitro.HandleClusterPublish", err, "Completed")
		return nil, true, err
	}

	route := data[0]
	head := data[1]

	switch {
	case bytes.Equal(head, netd.EndMessage):
		if !n.payloadMode() {
			err := errors.New("Invalid Publish payload mode state")
			n.Error(context, "Nitro.HandleClusterPublish", err, "Completed")
			return nil, true, err
		}

		n.payloadDown()
		croute := n.currentRoute
		pmb := n.payload.Bytes()

		n.payload.Reset()
		n.currentRoute = nil

		cx.Router.Handle(context, croute, pmb, *cx.Base)

	case bytes.Equal(head, netd.BeginMessage):
		n.currentRoute = route
		n.payloadUp()

	default:
		for _, hm := range data[1:] {
			n.Log(context, "Nitro.HandleClusterPublish", "Routing : Topic[%+s] : Message[%+s]", route, hm)
			cx.Router.Handle(context, route, hm, *cx.Base)
		}
	}

	n.Log(context, "Nitro.HandleClusterPublish", "Completed")
	return netd.OkMessage, false, nil
}

// HandlePayload handles gathering payload data into a complete sequence of data sets.
func (n *Nitro) HandlePayload(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandlePayload", "Started : Data : %+q", data)

	if len(data) == 0 {
		err := errors.New("Empty data received")
		n.Error(context, "Nitro.HandlePayload", err, "Completed")
		return nil, true, err
	}

	if !n.payloadMode() {
		err := errors.New("Invalid Data collection mode")
		n.Error(context, "Nitro.HandlePayload", err, "Completed")
		return nil, true, err
	}

	for _, da := range data {
		n.payload.Write(da)
	}

	if err := cx.SendToClusters(context, cx.Base.ClientID, netd.WrapResponseBlock(netd.PayloadMessage, data...), true); err != nil {
		n.Error(context, "Nitro.HandlePayload", err, "Completed")
		return nil, true, err
	}

	n.Log(context, "Nitro.HandlePayload", "Completed")
	return netd.OkMessage, false, nil
}

// HandleDeferMessage handles the process of the deffered message request.
func (n *Nitro) HandleDeferMessage(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleDeferMessage", "Started : Data[%s]", data)

	if len(data) == 0 {
		err := fmt.Errorf("Expected deferred request not empty data")
		n.Error(context, "Nitro.HandleDeferMessage", err, "Completed")
		return nil, true, err
	}

	var blocks []byte

	if len(data) < 2 {
		blocks = netd.WrapBlock(data[0])
	} else {
		blocks = netd.WrapResponseBlock(data[0], data[1:]...)
	}

	n.Log(context, "Nitro.HandleDeferMessage", "Info : Parsing Deferred Message : %+s", blocks)

	msgs, err := cx.Parser.Parse(blocks)
	if err != nil {
		n.Error(context, "Nitro.HandleDeferMessage", err, "Completed")
		return nil, true, err
	}

	n.Log(context, "Nitro.HandleDeferMessage", "Info : Generating  Message Structure: %#v", msgs)

	if err := cx.Defer(context, msgs...); err != nil {
		n.Error(context, "Nitro.HandleDeferMessage", err, "Completed")
		return nil, true, err
	}

	n.Log(context, "Nitro.HandleDeferMessage", "Completed")
	return nil, false, nil
}

// HandleConnectResponse handles the response recieved from the
func (n *Nitro) HandleConnectResponse(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleConnectResponse", "Started : Data[%s]", data)

	if len(data) < 1 {
		err := fmt.Errorf("Expected connect base info")
		n.Error(context, "Nitro.HandleConnectResponse", err, "Completed")
		return nil, true, err
	}

	var info netd.BaseInfo
	if err := json.Unmarshal(data[0], &info); err != nil {
		n.Error(context, "Nitro.HandleConnectResponse", err, "Completed")
		return nil, true, err
	}

	n.Log(context, "Nitro.HandleConnectResponse", "Info :  %s", info.ID())

	cx.Base.ServerID = info.ServerID
	cx.Base.ClusterNode = info.ClusterNode

	n.Log(context, "Nitro.HandleConnectResponse", "New Info :  %s", cx.Base.ID())

	clusterList := [][]byte{netd.ClusterMessage}

	clusters := cx.Connections.Clusters(context)
	for _, cluster := range clusters {
		if cx.Base.Match(cluster) {
			continue
		}

		clusterList = append(clusterList, []byte(fmt.Sprintf("%s:%d", cluster.RealAddr, cluster.RealPort)))
	}

	clreq := netd.WrapResponseBlock(netd.ClustersMessage, []byte(cx.Server.ServerID))
	csreq := netd.WrapResponseBlock(netd.DeferRequestMessage, clusterList...)

	n.Log(context, "Nitro.HandleConnectResponse", "Completed")
	return netd.WrapResponse(nil, clreq, csreq), false, nil
}

// HandleConnect handles the response of sending the server info recieved
// for the connect request.
func (n *Nitro) HandleConnect(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleConnect", "Started")

	if len(data) == 0 {
		err := fmt.Errorf("Expected requester ID")
		n.Error(context, "Nitro.HandleConnect", err, "Completed")
		return nil, true, err
	}

	clientID := string(data[0])
	n.Log(context, "Nitro.HandleConnect", "Info : Connect Request from Server %q", clientID)

	info, err := json.Marshal(cx.Server)
	if err != nil {
		n.Error(context, "Nitro.HandleConnect", err, "Completed")
		return nil, true, err
	}

	n.Log(context, "Nitro.HandleConnect", "Completed")
	return netd.WrapResponse(netd.ConnectResMessage, info), false, nil
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
	return netd.WrapResponse(netd.InfoResMessage, info), false, nil
}

// HandleCluster handles the cluster request, requesting the new cluster provided
// information be processed.
func (n *Nitro) HandleCluster(context interface{}, clusters [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleCluster", "Started")

	for _, cluster := range clusters {
		data := bytes.Split(cluster, colon)
		if len(data) < 2 {
			err := errors.New("Invalid Cluster Data, expected {CLUSTER|ADDR:PORT|ADDR:PORT}")
			n.Error(context, "Nitro.HandleCluster", err, "Completed")
			return nil, true, err
		}

		addr := string(data[0])

		port, err := strconv.Atoi(string(data[1]))
		if err != nil {
			err = fmt.Errorf("Port is not a number: " + err.Error())
			n.Error(context, "Nitro.HandleCluster", err, "Completed")
			return nil, true, err
		}

		if err := cx.ClusterConnect.NewCluster(context, addr, port); err != nil {
			n.Error(context, "Nitro.HandleCluster", err, "Completed")

			if err == netd.ErrAlreadyConnected {
				return nil, false, err
			}

			return nil, true, err
		}
	}

	n.Log(context, "Nitro.HandleCluster", "Completed")
	return netd.OkMessage, false, nil
}

// HandleClusters handles the requests of known clusters, which details all current
// available cluster connectins known by the provider.
func (n *Nitro) HandleClusters(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleClusters", "Started")

	if len(data) < 1 {
		err := fmt.Errorf("Expected requester ID")
		n.Error(context, "Nitro.HandleClusters", err, "Completed")
		return nil, true, err
	}

	clientID := data[0]

	cisd := string(clientID)
	if n.rcs[cisd] {
		n.Log(context, "Nitro.HandleClusters", "Info : Already Requsted Clusters : Source[%s]", cisd)

		delete(n.rcs, cisd)
		n.Log(context, "Nitro.HandleClusters", "Completed")
		return netd.OkMessage, false, nil
	}

	n.rcs[cisd] = true

	var clusterList [][]byte

	clusters := cx.Connections.Clusters(context)
	n.Log(context, "Nitro.HandleClusters", "Info : Total Clusters : Source[%s] : %d", cisd, len(clusters))

	for _, cluster := range clusters {
		if cx.Base.Match(cluster) {
			n.Log(context, "Nitro.HandleClusters", "Info : Found Self Referencing : {%#v} : Against : {%#v}", cluster, cx.Base)
			continue
		}

		clusterList = append(clusterList, []byte(fmt.Sprintf("%s:%d", cluster.RealAddr, cluster.RealPort)))
	}

	n.Log(context, "Nitro.HandleClusters", "Completed")
	return netd.WrapResponse(netd.ClusterMessage, netd.WrapBlockParts(clusterList)), false, nil
}

// HandleSubscriptions handles the requests of internal subscriptions list which details all current
// available subscriptions.
func (n *Nitro) HandleSubscriptions(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleSubscriptions", "Started")

	routes := cx.Router.Routes()

	n.Log(context, "Nitro.HandleSubscription", "Completed")
	return netd.WrapResponse(sub, netd.WrapBlockParts(routes)), false, nil
}

// HandleListTopicsRequest handles the LISTTOPICS request received from any client
// returning the topic lists alone. This defers from the SUBS request which returns
// the response in a SUB scription command. Use mainly for information only.
func (n *Nitro) HandleListTopicsRequest(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleSubscriptions", "Started")

	routes := cx.Router.Routes()

	n.Log(context, "Nitro.HandleSubscription", "Completed")
	return netd.WrapResponse(topics, netd.WrapBlockParts(routes)), false, nil
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
func (n *Nitro) HandleUnsubscribe(context interface{}, submsg [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleUnsubscribe", "Started : Subscribing to [%+q]", submsg)

	for _, sub := range submsg {
		if err := cx.Router.Unregister(sub, cx.Subscriber); err != nil {
			n.Error(context, "Nitro.HandleUnsubscribe", err, "Completed")
			return nil, false, err
		}
	}

	n.Log(context, "Nitro.HandleUnsubscribe", "Completed")
	return netd.WrapResponse(nil, netd.OkMessage), false, nil
}

// HandleErrors handles error responses from other connections, processing and
// deciding if need be to process.
func (n *Nitro) HandleErrors(context interface{}, data [][]byte, cx *netd.Connection) ([]byte, bool, error) {
	n.Log(context, "Nitro.HandleErrors", "Started : Handling Error Message [%+s]", data)

	if len(data) > 1 {
		return nil, true, errors.New("Expected only one error message")
	}

	if string(data[0]) == netd.ErrAlreadyConnected.Error() {
		n.Log(context, "Nitro.HandleErrors", "Info : Received Already Connected error message")
		return nil, false, nil
	}

	n.Log(context, "Nitro.HandleErrors", "Completed")
	return nil, true, nil
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
	case bytes.Equal(message.Command, netd.ConnectResMessage):
		return n.HandleConnectResponse(context, message.Data, cx)

	case bytes.Equal(message.Command, netd.ConnectMessage):
		return n.HandleConnect(context, message.Data, cx)

	case bytes.Equal(message.Command, netd.DeferRequestMessage):
		return n.HandleDeferMessage(context, message.Data, cx)

	case bytes.Equal(message.Command, netd.InfoMessage):
		return n.HandleInfo(context, cx)

	case bytes.Equal(message.Command, netd.ClustersMessage):
		return n.HandleClusters(context, message.Data, cx)

	case bytes.Equal(message.Command, netd.ClusterMessage):
		return n.HandleCluster(context, message.Data, cx)

	case bytes.Equal(message.Command, netd.DataMessage):
		return n.HandleData(context, message.Data, cx)

	case bytes.Equal(message.Command, clusterpub):
		return n.HandleClusterPublish(context, message.Data, cx)

	case bytes.Equal(message.Command, pub):
		return n.HandlePublish(context, message.Data, cx)

	case bytes.Equal(message.Command, subs):
		return n.HandleSubscriptions(context, message.Data, cx)

	case bytes.Equal(message.Command, listtopics):
		return n.HandleListTopicsRequest(context, message.Data, cx)

	case bytes.Equal(message.Command, netd.IdentityMessage):
		return n.HandleIdentity(context, message.Data, cx)

	case bytes.Equal(message.Command, sub):
		return n.HandleSubscribe(context, message.Data, cx)

	case bytes.Equal(message.Command, unsub):
		return n.HandleUnsubscribe(context, message.Data, cx)

	case bytes.Equal(message.Command, netd.PayloadMessage):
		return n.HandlePayload(context, message.Data, cx)

	case bytes.Equal(message.Command, netd.ErrMessage):
		return n.HandleErrors(context, message.Data, cx)

	case bytes.Equal(message.Command, netd.OkMessage):
		return nil, false, nil

	case bytes.Equal(message.Command, topics):
		return nil, false, nil

	case bytes.Equal(message.Command, exit):
		cx.Base.ExitedNormaly = true
		return netd.OkMessage, true, nil

	case bytes.Equal(message.Command, netd.InfoResMessage):
		return nil, false, nil

	case bytes.Equal(message.Command, netd.RespMessage):
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
	n.Log(context, "Nitro.Process", "Started : From{Client: %s, Server: %s} :  Messages {   %+q  }", cx.Base.ID(), cx.Server.ID(), messages)

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
