package netd

import (
	"bytes"
	"errors"
)

// BlockParser defines a package level parser using the blockMessage specification.
/* The block message specification is based on the idea of a simple text based
   messages which follows specific rules about messages which are:

   1. All messages must end with a CRTL line ending `\r\n`.
   2. All message must begin with a  opening(`{`) backet and close with a closing(`}`) closing bracket.
   3. Multiplex messages can be included together by combining messages wrapped by the brackets with a semicolon(`:`) in between.

*/
var BlockParser blockMessage

// blockMessage defines a struct for the blockParser.
type blockMessage struct{}

// Parse parses the data data coming in and produces a series of Messages
// based on a base pattern.
func (blockMessage) Parse(msg []byte) ([]Message, error) {
	var messages []Message

	return messages, nil
}

// SplitParts splits the parts of a message block divided by a vertical bar(`|`)
// symbol. It spits out the parts into their seperate enitites.
func (blockMessage) SplitParts(msg []byte) [][]byte {
	var dataBlocks [][]byte

	var block []byte
	msgLen := len(msg)

	var symbol byte
	var symbolFound bool
	var dashFound bool

	for i := 0; i < msgLen; i++ {
		item := msg[i]

		if item == '|' {
			if dashFound && !symbolFound {
				symbolFound = found
				continue
			}

			if dashFound && symbolFound {
			}

			dataBlocks = append(dataBlocks, block)
			dashFound = true
			block = nil
		}

		block = append(block, item)
	}

	return dataBlocks
}

// SplitMultiplex will split messages down into their separate parts by
// breaking patterns of message packs into their seperate parts.
// multiplex message: `{A|U|Runner}:{+SUBS|R|}\r\n` => []{[]byte("A|U|Runner}\r\n"), []byte("+SUBS|R|bucks\r\n")}.
func (blockMessage) SplitMultiplex(msg []byte) ([][]byte, error) {
	var blocks [][]byte

	var block []byte
	var messageStart bool

	msgLen := len(msg)

	{
	blockLoop:
		for i := 0; i < msgLen; i++ {
			item := msg[i]

			switch item {
			case ':':
				if i+1 >= msgLen {
					return nil, errors.New("Invalid Block arrangement. Blocks must follow {contents} rule.")
				}

				piece := msg[i-1]
				var nxtPiece []byte

				if i < msgLen && i+2 < msgLen {
					nxtPiece = msg[i : i+2]
				}

				if -1 != bytes.Compare(nxtPiece, beginColonBracket) && piece == endBracket {
					blocks = append(blocks, block)
					block = nil
					messageStart = false
					continue
				}

				block = append(block, item)
				continue

			case '{':
				if messageStart {
					block = append(block, item)
					continue
				}

				messageStart = true
				block = append(block, item)
				continue

			case '}':
				if !messageStart {
					return nil, errors.New("Invalid Start of block")
				}

				// Are we at message end and do are we starting a new block?
				if msg[i+1] == ':' && i+2 >= msgLen {
					return nil, errors.New("Invalid new Block start")
				}

				if msg[i+1] == ':' && msg[i+2] == '{' {
					block = append(block, item)
					blocks = append(blocks, block)
					block = nil
					messageStart = false
					continue
				}

				ctrl := msg[i+1 : msgLen]
				if 0 == bytes.Compare(ctrl, ctrlLine) {
					if item == endBracket {
						block = append(block, item)
					}

					blocks = append(blocks, block)
					block = nil
					break blockLoop
				}

				block = append(block, item)

			default:

				ctrl := msg[i:msgLen]
				if 0 == bytes.Compare(ctrl, ctrlLine) {
					break blockLoop
				}

				block = append(block, item)
			}
		}

	}

	if len(block) > 1 {
		blocks = append(blocks, block)
	}

	return blocks, nil
}
