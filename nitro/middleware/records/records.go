package records

// RecordMW is a struct which implements the netd.Middleware interface. It provides a new message
// processor for a RECORD message format that allows clients to send efficient record/model transactions
// over the wire.
type RecordWM struct{}
