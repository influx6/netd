package records

import "github.com/influx6/netd"

type Cache interface {
}

// RecordMW is a struct which implements the netd.Middleware interface. It provides a new message
// processor for a RECORD message format that allows clients to send efficient record/model transactions
// over the wire.
type RecordWM struct {
	cache Cache
}

// HandleEvents provides a method which implements the netd.Middleware interface
// and handles the subscription if needed for connect and disconnect events.
func (RecordWM) HandleEvents(context interface{}, cx netd.ConnectionEvents) error {

	return nil
}
