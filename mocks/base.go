package mocks

import (
	"bytes"

	"github.com/influx6/netd"
)

//==============================================================================

// ConnectionsMocks defines a mock for the netd.Connections interface.
type ConnectionsMock struct {
	ClientList      []netd.BaseInfo
	ClusterList     []netd.BaseInfo
	ClusterMessages bytes.Buffer
	ClientMessages  bytes.Buffer
}

// Clients implements the netd.Connections.Clients method.
func (c *ConnectionsMock) Clients(context interface{}) netd.SearchableInfo {
	return netd.SearchableInfo(c.ClientList)
}

// Clusters implements the netd.Connections.Clusters method.
func (c *ConnectionsMock) Clusters(context interface{}) netd.SearchableInfo {
	return netd.SearchableInfo(c.ClientList)
}

// SendToClusters implements the netd.Connections.SendToClusters method.
func (c *ConnectionsMock) SendToClusters(context interface{}, id string, msg []byte, flush bool) error {
	c.ClusterMessages.Write(msg)
	return nil
}

// SendToClient implements the netd.Connections.SendToClient method.
func (c *ConnectionsMock) SendToClients(context interface{}, id string, msg []byte, flush bool) error {
	c.ClientMessages.Write(msg)
	return nil
}

//==============================================================================

// SubscriberMock implements the netd.Subscriber interface.
type SubscriberMock struct {
	Receiver func(context interface{}, sm *netd.SubMessage) error
}

// Fire implements the Fire call for the subscriber.
func (s *SubscriberMock) Fire(context interface{}, sm *netd.SubMessage) error {
	return s.Receiver(context, sm)
}

//==============================================================================

// MessagerMock is a mock implementation for the netd.Messager interface.
type MessagerMock struct {
	EMessages bytes.Buffer
	NMessages bytes.Buffer
}

// Send implements the Send method for the netd.Messager.
func (s *MessagerMock) Send(context interface{}, flush bool, msgs ...[]byte) error {
	for _, msg := range msgs {
		if err := s.SendMessage(context, msg, flush); err != nil {
			return err
		}
	}

	return nil
}

// SendError implements the SendError method for the netd.Messager.
func (s *MessagerMock) SendError(context interface{}, flush bool, msgs ...error) error {
	for _, msg := range msgs {
		s.EMessages.Write([]byte(msg.Error()))
	}

	return nil
}

// SendMessage implements the SendMessage method for the netd.Messager.
func (s *MessagerMock) SendMessage(context interface{}, msg []byte, flush bool) error {
	s.NMessages.Write(msg)
	return nil
}

//==============================================================================

// DeferRequestMock implements a mock for the netd.DeferRequest interface.
type DeferRequestMock struct {
	DMessages []netd.Message
}

// Defer implements the netd.DeferRequest.Defer method.
func (d *DeferRequestMock) Defer(context interface{}, msg ...netd.Message) error {
	d.DMessages = append(d.DMessages, msg...)
	return nil
}

//==============================================================================

// ClusterMock implements the netd.ClusterConnect interface.
type ClusterMock struct{}

// NewCluster implements a function for creating new clusters for communicating
// between processes.
func (c *ClusterMock) NewCluster(context interface{}, addr string, port int) error {

	return nil
}

//==============================================================================
