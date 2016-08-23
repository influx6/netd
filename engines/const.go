package engines

import (
	"bytes"
	"encoding/json"
	"fmt"
)

var (
	ctrl        = "\r\n"
	spaceString = []byte(" ")
	emptyString = []byte("")
	endTrace    = []byte("End Trace")
	ctrlLine    = []byte(ctrl)
	newLine     = []byte("\n")

	// message types for different responses
	infoMessage = []byte("INFO")
	okMessage   = []byte("+OK\r\n")
	pingMessage = []byte("PING\r\n")
	pongMessage = []byte("P0NG\r\n")
	errMessage  = []byte("+ERR")

	// request message types
	sub      = []byte("SUB")
	unsub    = []byte("UNSUB")
	listSub  = []byte("LISTSB")
	msgBegin = []byte("MSG_PAYLOAD")
	msgEnd   = []byte("MSG_END")
)

func makeErr(message string, data ...interface{}) []byte {
	msg := []byte(fmt.Sprintf(message, data...))
	return bytes.Join([][]byte{errMessage, spaceString, msg, ctrlLine}, emptyString)
}

func toJSON(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}
