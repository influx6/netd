package nitro

import (
	"bytes"
	"errors"

	"github.com/influx6/netd"
)

var (

	// response message headers
	errMessage  = []byte("+ERR")
	respMessage = []byte("+RESP")

	// request message headers
	batchd      = []byte("+BATCHD")
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

	// error messages
	invalidInfoResponse = errors.New("Failed to unmarshal info data")
)

// NitroBase implements a netd.RequestResponse interface, providing methods to handle
// and manage the behaviour of a provider.
type NitroBase struct {
  netd.Logger
  netd.Trace

  Next netd.RequestResponse
}

// Process implements the netd.RequestResponse.Process method which process all
// incoming messages.
func (n *NitroBase) Process(context interface{}, cx *netd.Connection, messages ...netd.Messages) error {
  n.Log(context, "NitroBase.Process", "Started : From{Client: %q, Server: %q} : Messages {%d} : {%#v}", cx.Base.ClientID, cx.Server.ServerID, len(messages), messages)

  for _, message := range messages{
      cmd := message.Command

      switch {
       case bytes.Equal(cmd, connect):
         info, err := json.Marshal(rl.Connection.ServerInfo)
         if err != nil {
           rl.Config.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
           return err
         }

         rl.SendResponse(context, info, true)
       case bytes.Equal(cmd, info):
         info, err := json.Marshal(rl.BaseInfo())
         if err != nil {
           rl.Config.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
           return err
         }

         rl.SendResponse(context, info, true)
       case bytes.Equal(cmd, cluster):
         if dataLen != 2 {
           err := errors.New("Invalid Cluster Data, expected {CLUSTER|ADDR|PORT}")
           rl.SendError(context, invalidClusterInfo, true)
           rl.Config.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
           return err
         }

         addr := string(message.Data[0])
         port, err := strconv.Atoi(string(message.Data[1]))
         if err != nil {
           rl.SendError(context, fmt.Errorf("Port is not a int: "+err.Error()), true)
           rl.Config.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
           return err
         }

         if err := rl.Connections.NewClusterFromAddr(context, addr, port); err != nil {
           rl.SendError(context, fmt.Errorf("New Cluster Connect failed: "+err.Error()), true)
           rl.Config.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
           return err
         }

         return rl.SendResponse(context, okMessage, true)
       default:
        if err := n.Next.Process(context, cx, message); err != nil {
          return err 
        }
      }
    }

    return nil
}


// Nitro implements a netd.RequestResponse interface, providing methods to handle
// and manage the behaviour of a provider.
type Nitro struct {
	netd.Logger
	netd.Trace

	Next netd.RequestResponse
}

// HandleEvents registers event watchers with the provided listeners.
func (n *Nitro) HandleEvents(context interface{}, cx netd.ConnectionEvents) error {

	return nil
}

// Process implements the netd.RequestResponse.Process method which process all
// incoming messages.
func (n *Nitro) Process(context interface{}, cx *netd.Connection, messages ...netd.Messages) error {
  n.Log(context, "Nitro.Process", "Started : From{Client: %q, Server: %q} : Messages {%d} : {%#v}", cx.Base.ClientID, cx.Server.ServerID, len(messages), messages)

	var responses [][][]byte

	for _, message := range messages {

		switch {
		case bytes.Equal(message.Command, info):
			break
		}

	}

	if err := mx.Send(context, true, responses...); err != nil {
		n.Error(context, "Process", err, "Completed")
		return err
	}

	n.Log(context, "Process", "Completed")
	return nil
}

func (n *Nitro) Process(context interface{}, messages netd.Messages, cx *netd.Connection) error {
