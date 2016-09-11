package nitro

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

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
	pingMessage = []byte("PING")
	pongMessage = []byte("P0NG")
	connect     = []byte("CONNECT")
	msgEnd      = []byte("MSG_END")
	msgBegin    = []byte("MSG_PAYLOAD")
)

// Nitro implements a netd.RequestResponse interface, providing methods to handle
// and manage the behaviour of a provider.
type Nitro struct {
	netd.Logger
	netd.Trace

	Next netd.Middleware
}

// HandleEvents connects to the connection event provider to listening for
// connects and disconnects.
func (n *Nitro) HandleEvents(context interface{}, cx netd.ConnectionEvents) error {
	if n.Next != nil {
		return n.Next.HandleEvents(context, cx)
	}

	return nil
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
	return netd.WrapResponse(netd.RespMessage, netd.OkMessage), false, nil
}

func (n *Nitro) HandleMessage(context interface{}, cx *netd.Connection, message netd.Message) ([]byte, bool, error) {
	switch {
	case bytes.Equal(message.Command, netd.ConnectMessage):
		return n.HandleConnect(context, cx)
	case bytes.Equal(message.Command, netd.InfoMessage):
		return n.HandleInfo(context, cx)
	case bytes.Equal(message.Command, netd.ClusterMessage):
		return n.HandleCluster(context, message.Data, cx)
	default:
		if n.Next != nil {
			return n.Next.Handle(context, message, cx)
		}

		return nil, true, netd.ErrInvalidRequest
	}
}

// Process implements the netd.RequestResponse.Process method which process all
// incoming messages.
func (n *Nitro) Process(context interface{}, cx *netd.Connection, messages ...netd.Message) (bool, error) {
	n.Log(context, "Nitro.Process", "Started : From{Client: %q, Server: %q} : Messages {%d} : {%#v}", cx.Base.ClientID, cx.Server.ServerID, len(messages), messages)

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
