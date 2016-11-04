package mocks

import (
	"bytes"
	"fmt"
	"net"
	"strconv"

	. "github.com/influx6/netd"
	"github.com/pborman/uuid"
)

//==============================================================================

// NewConnection returns a mock Connection instance which provides a mock implementation
// for use with netd.Providers.
func NewConnection(client, server *BaseInfo, receiver Receiver, router Router, parser MessageParser) *Connection {
	return &Connection{
		Base:         client,
		Server:       server,
		Parser:       parser,
		Router:       router,
		Stat:         &Stat{},
		Connections:  &ConnectionsMock{},
		Messager:     &MessagerMock{},
		Subscriber:   &SubscriberMock{Receiver: receiver},
		DeferRequest: &DeferRequestMock{},
	}
}

// MockConn returns a new Connection using the giving addr.
func MockConn(addr string, receiver Receiver, router Router) *Connection {
	var info BaseInfo

	ip, pr, _ := net.SplitHostPort(addr)
	port, _ := strconv.Atoi(pr)

	info.Addr = ip
	info.Port = port
	info.RealAddr = ip
	info.RealPort = port
	info.LocalAddr = ip
	info.LocalPort = port
	info.ServerID = uuid.New()
	info.ClientID = uuid.New()

	return NewConnection(&info, &info, receiver, router, BlockParser)
}

//==============================================================================

// Pipe connects two *Connections together to allow messages to be sent between each.
func Pipe(src, dest *Connection, handler RequestResponse) (*MessagePipe, *MessagePipe) {
	var srcm MessagePipe
	srcm.Cx = dest
	srcm.ReRs = handler
	srcm.M = src.Messager.(*MessagerMock)

	var destm MessagePipe
	destm.Cx = src
	destm.ReRs = handler
	destm.M = dest.Messager.(*MessagerMock)

	return &srcm, &destm
}

// MessagePipe defines a struct which connects  a  destination Message and passes all  to its RequestResponse provider.
type MessagePipe struct {
	Cx          *Connection
	M           *MessagerMock
	ReRs        RequestResponse
	ShouldClose bool
}

// Send implements the Send method for the Messager.
func (mp *MessagePipe) Send(context interface{}, flush bool, msgs ...[]byte) error {
	resMsg := WrapResponse(nil, msgs...)
	if err := mp.SendMessage(context, resMsg, true); err != nil {
		return err
	}

	return nil
}

// SendError implements the SendError method for the Messager.
func (mp *MessagePipe) SendError(context interface{}, flush bool, msgs ...error) error {
	var errs [][]byte

	for _, err := range msgs {
		errs = append(errs, []byte(err.Error()))
	}

	errMsg := WrapResponse(ErrMessage, errs...)
	if err := mp.SendMessage(context, errMsg, true); err != nil {
		return err
	}

	return nil
}

// SendMessage implements the SendMessage method for the Messager.
func (mp *MessagePipe) SendMessage(context interface{}, msg []byte, flush bool) error {
	if err := mp.M.SendMessage(context, msg, flush); err != nil {
		return err
	}

	messages, err := mp.Cx.Parser.Parse(msg)
	if err != nil {
		return err
	}

	mp.ShouldClose, err = mp.ReRs.Process(context, mp.Cx, messages...)
	if err != nil {
		return err
	}

	return nil
}

//==============================================================================

// ConnectionsMocks defines a mock for the Connections interface.
type ConnectionsMock struct {
	ClientList      []BaseInfo
	ClusterList     []BaseInfo
	ClusterMessages bytes.Buffer
	ClientMessages  bytes.Buffer
}

// Clients implements the Connections.Clients method.
func (c *ConnectionsMock) Clients(context interface{}) SearchableInfo {
	return SearchableInfo(c.ClientList)
}

// Clusters implements the Connections.Clusters method.
func (c *ConnectionsMock) Clusters(context interface{}) SearchableInfo {
	return SearchableInfo(c.ClientList)
}

// SendToClusters implements the Connections.SendToClusters method.
func (c *ConnectionsMock) SendToClusters(context interface{}, id string, msg []byte, flush bool) error {
	c.ClusterMessages.Write(msg)
	return nil
}

// SendToClient implements the Connections.SendToClient method.
func (c *ConnectionsMock) SendToClients(context interface{}, id string, msg []byte, flush bool) error {
	c.ClientMessages.Write(msg)
	return nil
}

// MessagerMock is a mock implementation for the Messager interface.
type MessagerMock struct {
	m [][]byte
}

// Reset sets the list to nil.
func (s *MessagerMock) Reset() {
	s.m = nil
}

// Shift returns the first item in the byte list.
func (s *MessagerMock) Shift() []byte {
	last := s.m[0]

	s.m = s.m[1:]
	return last
}

// Pop returns the last item in the byte list.
func (s *MessagerMock) Pop() []byte {
	lastIndex := len(s.m) - 1
	last := s.m[lastIndex]

	s.m = s.m[:lastIndex]
	return last
}

// Send implements the Send method for the Messager.
func (s *MessagerMock) Send(context interface{}, flush bool, msgs ...[]byte) error {
	for _, msg := range msgs {
		s.SendMessage(context, msg, flush)
	}

	return nil
}

// String returns the internal contents of the byte slice as a string.
func (s *MessagerMock) String() string {
	return fmt.Sprintf("%+q", s.m)
}

// SendError implements the SendError method for the Messager.
func (s *MessagerMock) SendError(context interface{}, flush bool, msgs ...error) error {
	for _, msg := range msgs {
		s.SendMessage(context, WrapResponse(ErrMessage, []byte(msg.Error())), flush)
	}

	return nil
}

// SendMessage implements the SendMessage method for the Messager.
func (s *MessagerMock) SendMessage(context interface{}, msg []byte, flush bool) error {
	s.m = append(s.m, msg)
	return nil
}

//==============================================================================

// DeferRequestMock implements a mock for the DeferRequest interface.
type DeferRequestMock struct {
	DMessages []Message
}

// Defer implements the DeferRequest.Defer method.
func (d *DeferRequestMock) Defer(context interface{}, msg ...Message) error {
	d.DMessages = append(d.DMessages, msg...)
	return nil
}

//==============================================================================

// ClusterMock implements the ClusterConnect interface.
type ClusterMock struct{}

// NewCluster implements a function for creating new clusters for communicating
// between processes.
func (c *ClusterMock) NewCluster(context interface{}, addr string, port int) error {
	return nil
}

//==============================================================================

// Receiver defines the function type for Submessage reception.
type Receiver func(context interface{}, sm *SubMessage) ([]byte, error)

// SubscriberMock implements the Subscriber interface.
type SubscriberMock struct {
	Response []byte
	Receiver Receiver
}

// Fire implements the Fire call for the subscriber.
func (s *SubscriberMock) Fire(context interface{}, sm *SubMessage) error {
	res, err := s.Receiver(context, sm)
	if err != nil {
		return err
	}

	s.Response = res
	return nil
}

//==============================================================================
