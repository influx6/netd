package netd_test

import (
	"fmt"
	"testing"

	"github.com/influx6/netd"
)

// succeedMark is the Unicode codepoint for a check mark.
const succeedMark = "\u2713"

// failedMark is the Unicode codepoint for an X mark.
const failedMark = "\u2717"

func TestBlockMessageParser(t *testing.T) {
	messages, err := netd.BlockParser.Parse([]byte(`{A|U|Runner}:{+SUBS|R|}\r\n`))
	if err != nil {
		fatalFailed(t, "Should have parsed message blocks: %s", err)
	}
	logPassed(t, "Should have parsed message blocks: %#q", messages)
}

func TestParserBlocks(t *testing.T) {
	blocks, err := netd.BlockParser.SplitMultiplex([]byte(`{A|U|Runner}:{+SUBS|R|}\r\n`))
	if err != nil {
		fatalFailed(t, "Should have parsed blocks: %s", err)
	}
	logPassed(t, "Should have parsed blocks: %+s", blocks)

	firstBlock := netd.BlockParser.SplitParts(blocks[0])
	if len(firstBlock) != 3 {
		fatalFailed(t, "Should have parsed block[%+s] into 3 parts: %+s", blocks[0], firstBlock)
	}

	logPassed(t, "Should have parsed block[%+s] into 3 parts: %+s", blocks[0], firstBlock)
}

func TestBadBlock(t *testing.T) {
	block, err := netd.BlockParser.SplitMultiplex([]byte(`{A|D|"{udss\n\r}":`))
	if err != nil {
		logPassed(t, "Should have failed to parse blocks: %+s", err)
		return
	}

	fatalFailed(t, "Should have failed to parse blocks: %+s", block)
}

func TestBadParserBlocks(t *testing.T) {
	_, err := netd.BlockParser.SplitMultiplex([]byte(`{A|D|"{udss\n\r}"}:`))
	if err != nil {
		logPassed(t, "Should have failed to parse blocks: %+s", err)
		return
	}

	fatalFailed(t, "Should have failed to parse blocks")
}

func TestComplexParserBlocks(t *testing.T) {
	blocks, err := netd.BlockParser.SplitMultiplex([]byte(`{A|D|"{udss\n\r}"}:{+SUBS|R|}\r\n`))
	if err != nil {
		fatalFailed(t, "Should have parsed blocks: %s", err)
	}

	logPassed(t, "Should have parsed blocks: %+s", blocks)
}

func logPassed(t *testing.T, msg string, data ...interface{}) {
	t.Logf("%s %s", fmt.Sprintf(msg, data...), succeedMark)
}

func fatalFailed(t *testing.T, msg string, data ...interface{}) {
	t.Fatalf("%s %s", fmt.Sprintf(msg, data...), failedMark)
}
