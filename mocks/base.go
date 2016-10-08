package mocks

import (
	"bytes"

	. "github.com/influx6/netd"
)

//==============================================================================

// NewConnection returns a mock Connection instance which provides a mock implementation
// for use with netd.Providers.
func NewConnection(client, server *BaseInfo, router Router, receiver Receiver, stat StatProvider, parser MessageParser) *Connection {
	return &Connection{
		Base:         client,
		Server:       server,
		Router:       router,
		Stat:         stat,
		Parser:       parser,
		Connections:  &ConnectionsMock{},
		Subscriber:   &SubscriberMock{Receiver: receiver},
		Messager:     &MessagerMock{},
		DeferRequest: &DeferRequestMock{},
	}
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

//==============================================================================

// Receiver defines the function type for Submessage reception.
type Receiver func(context interface{}, sm *SubMessage) error

// SubscriberMock implements the Subscriber interface.
type SubscriberMock struct {
	Receiver Receiver
}

// Fire implements the Fire call for the subscriber.
func (s *SubscriberMock) Fire(context interface{}, sm *SubMessage) error {
	return s.Receiver(context, sm)
}

//==============================================================================

// MessagerMock is a mock implementation for the Messager interface.
type MessagerMock struct {
	EMessages bytes.Buffer
	NMessages bytes.Buffer
}

// Send implements the Send method for the Messager.
func (s *MessagerMock) Send(context interface{}, flush bool, msgs ...[]byte) error {
	for _, msg := range msgs {
		if err := s.SendMessage(context, msg, flush); err != nil {
			return err
		}
	}

	return nil
}

// SendError implements the SendError method for the Messager.
func (s *MessagerMock) SendError(context interface{}, flush bool, msgs ...error) error {
	for _, msg := range msgs {
		s.EMessages.Write([]byte(msg.Error()))
	}

	return nil
}

// SendMessage implements the SendMessage method for the Messager.
func (s *MessagerMock) SendMessage(context interface{}, msg []byte, flush bool) error {
	s.NMessages.Write(msg)
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
