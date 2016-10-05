package types

// Versions provides an interface for a sytem to generate the version provider
// which generates versions and matches versions for operations.
type Versions interface {
	New()
	Test(target, baseline string) bool
}

// Cache defines the interface for all cache items which will be used in the caching record values.
type Cache interface {
	Put(Record) error
	Exists(string) bool
	Delete(string) error
	Replace(Record) error
	Get(string) (Record, error)
	Patch(delta Record) (Record, error)
}

// Backend defines an interface which allows exposing a front through which records
// can be retrieved for caching and responses for operations.
type Backend interface {
	Put(Record) error
	Exists(string) bool
	Update(Record) error
	All() ([]Record, error)
	Get(string) (Record, error)
	Delete(string) (Record, error)
}

// Delta defines a base level data store which contains a replace/add instruction
// for a Record.
type Delta struct {
	Path     string `json:"path"`
	NewValue string `json:"new_value"`
}

// Record defines a generic interface which defines the underline record type received.
type Record struct {
	Version string                 `json:"version"`
	ID      string                 `json:"record_id"`
	Name    string                 `json:"record_name"`
	Deleted bool                   `json:"deleted"`
	Data    map[string]interface{} `json:"record_data"`
	Deltas  []Delta                `json:"deltas,omitempty"`
}

// BaseRecord defines the response/request recieved for a specific operation.
type BaseRecord struct {
	Status       bool   `json:"status"`
	Record       Record `json:"records"`
	ServerID     string `json:"server_id"`
	ClientID     string `json:"client_id"`
	Processed    bool   `json:"processed"`
	FromServerID string `json:"from_server_id,omitempty"`
	FromClientID string `json:"from_client_id,omitempty"`
	Error        string `json:"error,omitempty"`
}

// BaseRecords defines the type of slice BaseRecord type.
type BaseRecords []BaseRecord

// ReplaceRecord defines the response/request recieved for a replace operation.
type ReplaceRecord struct {
	New          Record `json:"new"`
	Old          Record `json:"old"`
	Status       bool   `json:"status"`
	Processed    bool   `json:"processed"`
	ServerID     string `json:"server_id"`
	ClientID     string `json:"client_id"`
	FromServerID string `json:"from_server_id,omitempty"`
	FromClientID string `json:"from_client_id,omitempty"`
}

// ReplaceRecords defines the type of slice BaseRecord type.
type ReplaceRecords []ReplaceRecords

// DeleteRecord defines the response/request recieved for a delete operation.
type DeleteRecord struct {
	DeleteID     string `json:"delete_id"`
	Status       bool   `json:"status"`
	Processed    bool   `json:"processed"`
	ServerID     string `json:"server_id"`
	ClientID     string `json:"client_id"`
	FromServerID string `json:"from_server_id,omitempty"`
	FromClientID string `json:"from_client_id,omitempty"`
}

// DeleteRecords defines the type of slice DeleteRecord type.
type DeleteRecords []DeleteRecord

// ReadRecord defines the response/request recieved for a read operation.
type ReadRecord struct {
	Records      []string `json:"records"`
	Status       bool     `json:"status"`
	Processed    bool     `json:"processed"`
	ServerID     string   `json:"server_id"`
	ClientID     string   `json:"client_id"`
	FromServerID string   `json:"from_server_id,omitempty"`
	FromClientID string   `json:"from_client_id,omitempty"`
}
