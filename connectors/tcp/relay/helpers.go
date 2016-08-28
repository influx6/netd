package relay

import (
	"bytes"
	"encoding/json"
	"fmt"
)

var (
	emptyString = []byte("")
	barLine     = []byte("|")
)

func makeErr(message string, data ...interface{}) []byte {
	msg := []byte(fmt.Sprintf(message, data...))
	return bytes.Join([][]byte{errMessage, spaceString, msg, ctrlLine}, emptyString)
}

func makeMessage(command []byte, messages ...[]byte) []byte {
	header := append([][]byte{bytes.ToUpper(command)}, messages...)
	return bytes.Join(header, barLine)
}

func wrapBlock(msg []byte) []byte {
	return bytes.Join([][]byte{[]byte("{"), msg, []byte("}")}, emptyString)
}

func toJSON(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func stripCTRL(msg []byte) []byte {
	return bytes.TrimSuffix(msg, ctrlLine)
}
