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

func TestParserBlocks(t *testing.T) {
	blocks, err := netd.BlockParser.SplitMultiplex([]byte(`{A|U|Runner}:{+SUBS|R|}\r\n`))
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
