package records

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/influx6/netd"
	"github.com/influx6/netd/middleware/records/types"
)

var (
	// ErrRecordExists is returned when a operation requires a non existing record
	// before running but finds the record id already exists.
	ErrRecordExists = errors.New("Record with given ID already exists")

	// ErrNoRecordFound is returned when an operation expects a record existing
	// with the provided id but was not found in cache or backend.
	ErrNoRecordFound = errors.New("Record with given ID already exists")

	// ErrInvalidPayloadState is returned when a invalid end state is received when no
	// begin phase is found.
	ErrInvalidPayloadState = errors.New("Invalid end message received")

	// ErrCanNotProcess is returned when an operation is not processable.
	ErrCanNotProcess = errors.New("Incapable to process operation")

	// ErrVersionConflict is returned when the record version does not match, as the
	// records are required to have a always incrementing version to reduce and
	// ensure consistency in transformations and operations on records.
	ErrVersionConflict = errors.New("Record versions are in conflict unable to perform operation")

	// RecordResponseMessage is used to send a reply back to the requestee after
	// processing the needed requests.
	RecordResponseMessage = []byte("RecordResponse")

	// CreateMessage defines the header name for create requests.
	CreateMessage = []byte("CREATE")

	// PatchMessage defines the header name for patch requests.
	PatchMessage = []byte("PATCH")

	// ReplaceMessage defines the header name for replace requests.
	ReplaceMessage = []byte("REPLACE")

	// ReadMessage defines the header name for read requests.
	ReadMessage = []byte("READ")

	// DeleteMessage defines the header name for delete requests.
	DeleteMessage = []byte("DELETE")
)

// bufferRecord defines a base level record buffering collector useful for operations
// that requires collection of streaming data.
type bufferRecord struct {
	bu bytes.Buffer
	on bool
}

// RecordMW returns a delegation which implements the netd.Middleware interface.
// It provides a new message processor for a RECORD message format that allows
// clients to send efficient record/model transactions over the wire.
func RecordMW(tracer netd.Trace, logger netd.Logger, versions types.Versions, cache types.Cache, backend types.Backend, ev ...netd.EventHandler) netd.Delegation {
	du := netd.NewDelegation()

	var createBuffer bufferRecord
	var replaceBuffer bufferRecord
	var deleteBuffer bufferRecord
	var readBuffer bufferRecord
	var patchBuffer bufferRecord

	// CREATE handles all create requests from the backend to create a record.
	du.Action("CREATE", func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {
		logger.Log(context, "RecordMW.CREATE", "Started : Message[%+q]", m)

		if len(m.Data) == 0 {
			logger.Error(context, "RecordMW.CREATE", netd.ErrEmptyData, "Completed")
			return nil, true, netd.ErrEmptyData
		}

		switch {
		case bytes.Equal(m.Data[0], netd.BeginMessage):
			createBuffer.on = true
			for _, mdata := range m.Data[1:] {
				createBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.CREATE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.PayloadMessage):
			for _, mdata := range m.Data[1:] {
				createBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.CREATE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.EndMessage):
			if !createBuffer.on {
				logger.Error(context, "RecordMW.CREATE", ErrInvalidPayloadState, "Completed")
				return nil, true, ErrInvalidPayloadState
			}

			createBuffer.on = false
		default:
			for _, mdata := range m.Data {
				createBuffer.bu.Write(mdata)
			}
		}

		data := createBuffer.bu.Bytes()
		createBuffer.bu.Reset()

		var rec types.BaseRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		// Check if record already exists in cache, if so then these means its violated
		// new record policy for create.
		if cache.Exists(rec.Record.ID) {
			logger.Error(context, "RecordMW.CREATE", ErrRecordExists, "Completed")
			return nil, true, ErrRecordExists
		}

		// Store record in backend and if error'd out then return err and close
		// connection.
		if err := backend.Put(rec.Record); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		// Store record in cache for quick access.
		if err := cache.Put(rec.Record); err != nil {
			logger.Error(context, "RecordMW.CREATE", err, "Completed")
			return nil, true, err
		}

		logger.Log(context, "RecordMW.CREATE", "Completed")
		return netd.OkMessage, false, nil
	})

	// REPLACE handles all replace requests from the backend to create a record.
	du.Action("REPLACE", func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {
		logger.Log(context, "RecordMW.REPLACE", "Started : Message[%+q]", m)

		if len(m.Data) == 0 {
			logger.Error(context, "RecordMW.REPLACE", netd.ErrEmptyData, "Completed")
			return nil, true, netd.ErrEmptyData
		}

		switch {
		case bytes.Equal(m.Data[0], netd.BeginMessage):
			replaceBuffer.on = true
			for _, mdata := range m.Data[1:] {
				replaceBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.REPLACE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.PayloadMessage):
			for _, mdata := range m.Data[1:] {
				replaceBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.REPLACE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.EndMessage):
			if !replaceBuffer.on {
				logger.Error(context, "RecordMW.REPLACE", ErrInvalidPayloadState, "Completed")
				return nil, true, ErrInvalidPayloadState
			}

			replaceBuffer.on = false
		default:
			for _, mdata := range m.Data {
				replaceBuffer.bu.Write(mdata)
			}
		}

		data := replaceBuffer.bu.Bytes()
		replaceBuffer.bu.Reset()

		var rec types.BaseRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error(context, "RecordMW.REPLACE", err, "Completed")
			return nil, true, err
		}

		if cache.Exists(rec.Record.ID) {
			if err := cache.Delete(rec.Record.ID); err != nil {
				logger.Error(context, "RecordMW.REPLACE", err, "Completed")
				return nil, true, err
			}
		}

		// Store record in backend and if error'd out then return err and close
		// connection.
		oldRec, err := backend.Delete(rec.Record.ID)
		if err != nil {
			logger.Error(context, "RecordMW.REPLACE", err, "Completed")
			return nil, true, err
		}

		// Store record in cache for quick access.
		if err := cache.Put(rec.Record); err != nil {
			logger.Error(context, "RecordMW.REPLACE", err, "Completed")
			return nil, true, err
		}

		responseJSON, err := json.Marshal(&types.ReplaceRecord{
			Status:       true,
			Processed:    true,
			Old:          oldRec,
			New:          rec.Record,
			ClientID:     cx.Base.ClientID,
			ServerID:     cx.Base.ServerID,
			FromClientID: rec.FromClientID,
			FromServerID: rec.FromServerID,
		})

		if err != nil {
			logger.Error(context, "RecordMW.REPLACE", err, "Completed")
			return nil, true, err
		}

		logger.Log(context, "RecordMW.REPLACE", "Completed")
		return netd.WrapResponseBlock(RecordResponseMessage, ReplaceMessage, responseJSON), false, nil
	})

	// DELETE handles all replace requests from the backend to create a record.
	du.Action("DELETE", func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {
		logger.Log(context, "RecordMW.DELETE", "Started : Message[%+q]", m)

		if len(m.Data) == 0 {
			logger.Error(context, "RecordMW.DELETE", netd.ErrEmptyData, "Completed")
			return nil, true, netd.ErrEmptyData
		}

		switch {
		case bytes.Equal(m.Data[0], netd.BeginMessage):
			deleteBuffer.on = true
			for _, mdata := range m.Data[1:] {
				deleteBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.DELETE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.PayloadMessage):
			for _, mdata := range m.Data[1:] {
				deleteBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.DELETE", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.EndMessage):
			if !deleteBuffer.on {
				logger.Error(context, "RecordMW.DELETE", ErrInvalidPayloadState, "Completed")
				return nil, true, ErrInvalidPayloadState
			}

			deleteBuffer.on = false
		default:
			for _, mdata := range m.Data {
				deleteBuffer.bu.Write(mdata)
			}
		}

		data := deleteBuffer.bu.Bytes()
		deleteBuffer.bu.Reset()

		var rec types.DeleteRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error(context, "RecordMW.DELETE", err, "Completed")
			return nil, true, err
		}

		var foundInCache bool
		var record types.Record

		if cache.Exists(rec.DeleteID) {

			cacheRec, err := cache.Get(rec.DeleteID)
			if err != nil {
				logger.Error(context, "RecordMW.DELETE", err, "Completed")
				return nil, true, err
			}

			record = cacheRec
			foundInCache = true

			if err := cache.Delete(rec.DeleteID); err != nil {
				logger.Error(context, "RecordMW.DELETE", err, "Completed")
				return nil, true, err
			}
		}

		backendRec, err := backend.Delete(rec.DeleteID)
		if err != nil {
			logger.Error(context, "RecordMW.DELETE", err, "Info : Delete Failed for ID[%s]", rec.DeleteID)
			return nil, true, err
		}

		if !foundInCache {
			record = backendRec
		}

		responseJSON, err := json.Marshal(&types.BaseRecord{
			Status:       true,
			Processed:    true,
			Record:       record,
			ClientID:     cx.Base.ClientID,
			ServerID:     cx.Base.ServerID,
			FromClientID: rec.FromClientID,
			FromServerID: rec.FromServerID,
		})

		if err != nil {
			logger.Error(context, "RecordMW.DELETE", err, "Completed")
			return nil, true, err
		}

		logger.Log(context, "RecordMW.DELETE", "Completed")
		return netd.WrapResponseBlock(RecordResponseMessage, DeleteMessage, responseJSON), false, nil
	})

	// GET/READ handles all read requests from the backend to create a record.
	du.Action("READ", func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {
		logger.Log(context, "RecordMW.READ", "Started : Message[%+q]", m)

		if len(m.Data) == 0 {
			logger.Error(context, "RecordMW.READ", netd.ErrEmptyData, "Completed")
			return nil, true, netd.ErrEmptyData
		}

		switch {
		case bytes.Equal(m.Data[0], netd.BeginMessage):
			readBuffer.on = true
			for _, mdata := range m.Data[1:] {
				readBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.READ", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.PayloadMessage):
			for _, mdata := range m.Data[1:] {
				readBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.READ", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.EndMessage):
			if !readBuffer.on {
				logger.Error(context, "RecordMW.READ", ErrInvalidPayloadState, "Completed")
				return nil, true, ErrInvalidPayloadState
			}

			readBuffer.on = false
		default:
			for _, mdata := range m.Data {
				readBuffer.bu.Write(mdata)
			}
		}

		data := readBuffer.bu.Bytes()
		readBuffer.bu.Reset()

		var rec types.ReadRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error(context, "RecordMW.READ", err, "Completed")
			return nil, true, err
		}

		var records types.BaseRecords

		for _, id := range rec.Records {
			var status bool

			if cache.Exists(id) {

				record, err := cache.Get(id)
				if err == nil {
					status = true
				}

				records = append(records, types.BaseRecord{
					Status:       status,
					Record:       record,
					Processed:    true,
					Error:        err.Error(),
					ServerID:     cx.Base.ServerID,
					ClientID:     cx.Base.ClientID,
					FromClientID: rec.FromClientID,
					FromServerID: rec.FromServerID,
				})

				continue
			}

			record, err := backend.Get(id)
			if err == nil {
				status = true
			}

			records = append(records, types.BaseRecord{
				Status:       status,
				Record:       record,
				Processed:    true,
				Error:        err.Error(),
				ServerID:     cx.Base.ServerID,
				ClientID:     cx.Base.ClientID,
				FromClientID: rec.FromClientID,
				FromServerID: rec.FromServerID,
			})
		}

		responseJSON, err := json.Marshal(&records)
		if err != nil {
			logger.Error(context, "RecordMW.READ", err, "Completed")
			return nil, true, err
		}

		logger.Log(context, "RecordMW.READ", "Completed")
		return netd.WrapResponseBlock(RecordResponseMessage, ReadMessage, responseJSON), false, nil
	})

	// PATCH handles all patch requests from the backend to patch/update a record.
	du.Action("PATCH", func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {
		logger.Log(context, "RecordMW.PATCH", "Started : Message[%+q]", m)

		if len(m.Data) == 0 {
			logger.Error(context, "RecordMW.PATCH", netd.ErrEmptyData, "Completed")
			return nil, true, netd.ErrEmptyData
		}

		switch {
		case bytes.Equal(m.Data[0], netd.BeginMessage):
			patchBuffer.on = true
			for _, mdata := range m.Data[1:] {
				patchBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.PATCH", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.PayloadMessage):
			for _, mdata := range m.Data[1:] {
				patchBuffer.bu.Write(mdata)
			}

			logger.Log(context, "RecordMW.PATCH", "Completed")
			return netd.OkMessage, false, nil

		case bytes.Equal(m.Data[0], netd.EndMessage):
			if !patchBuffer.on {
				logger.Error(context, "RecordMW.PATCH", ErrInvalidPayloadState, "Completed")
				return nil, true, ErrInvalidPayloadState
			}

			patchBuffer.on = false
		default:
			for _, mdata := range m.Data {
				patchBuffer.bu.Write(mdata)
			}
		}

		data := patchBuffer.bu.Bytes()
		patchBuffer.bu.Reset()

		var rec types.BaseRecord
		if err := json.Unmarshal(data, &rec); err != nil {
			logger.Error(context, "RecordMW.PATCH", err, "Completed")
			return nil, true, err
		}

		if cache.Exists(rec.Record.ID) {
			if err := cache.Delete(rec.Record.ID); err != nil {
				logger.Error(context, "RecordMW.PATCH", err, "Completed")
				return nil, true, err
			}
		}

		var responseJSON []byte

		logger.Log(context, "RecordMW.PATCH", "Completed")
		return netd.WrapResponseBlock(RecordResponseMessage, ReplaceMessage, responseJSON), false, nil
	})

	return netd.NewDelegation(ev...).
		Next("RECORDS", du).
		Action(string(RecordResponseMessage), func(context interface{}, m netd.Message, cx *netd.Connection) ([]byte, bool, error) {

			return nil, false, nil
		})
}

//==============================================================================

var (
	arrayStarter  = []byte("[")
	arrayEnd      = []byte("]")
	objectStarter = []byte("{")
	objectEnd     = []byte("}")
)

func isArrayJSON(b []byte) bool {
	return bytes.HasPrefix(b, arrayStarter) && bytes.HasSuffix(b, arrayEnd)
}

func isObjectJSON(b []byte) bool {
	return bytes.HasPrefix(b, objectStarter) && bytes.HasSuffix(b, objectEnd)
}

//==============================================================================

func patchWithPaths(patcher types.Record, record types.Record) error {
	if !patcher.IsDelta {
		return errors.New("Patcher record is not a delta")
	}

	for keys, value := range patcher.Data {
		keys := strings.Split(keys, ".")

	}

	return nil
}

func root(target interface{}, ks []string) (interface{}, bool) {
	if len(ks) == 0 {
		return target, true
	}

	key := ks[0]

	intKey, err := strconv.Atoi(key)
	if err == nil {
		switch boTarget := target.(type) {
		case []int:
			return boTarget[intKey], true
		case []uint8:
			return boTarget[intKey], true
		case []uint64:
			return boTarget[intKey], true
		case []uint32:
			return boTarget[intKey], true
		case []uint16:
			return boTarget[intKey], true
		case []float64:
			return boTarget[intKey], true
		case []float32:
			return boTarget[intKey], true
		case []string:
			return boTarget[intKey], true
		case []interface{}:
			return boTarget[intKey], true
		}

		return nil, false
	}

	switch mdl := target.(type) {

	case map[string]interface{}:
		return root(mdl, ks[1:])

	case map[string]string:
		remKey := ks[1:]
		if len(remKey) > 1 {
			return nil, false
		}

		val, ok := mdl[ks[1]]
		return val, ok
	}

	return nil, false
}
