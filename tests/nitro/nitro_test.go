package nitro

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/influx6/netd"
	"github.com/influx6/netd/mocks"
	"github.com/influx6/netd/nitro"
	"github.com/influx6/netd/routes"
	"github.com/influx6/netd/tests"
)

var context = "testing"
var log = mocks.NewMockLogger(os.Stdout)
var tracer = mocks.NewMockTracer(os.Stdout)

func TestNitro(t *testing.T) {
	t.Logf("Given the need to use Nitro has the base message processor for netd ")
	{
		src, dest := newConnection()
		testInfo(t, src, dest)
		testConnect(t, src, dest)
	}
}

func testConnect(t *testing.T, src *mocks.MessagePipe, dest *mocks.MessagePipe) {
	t.Logf("\tWhen requesting CONNECT command")
	{

		defer src.M.Reset()
		defer dest.M.Reset()

		data := netd.WrapResponse(netd.ConnectMessage, []byte(dest.Cx.Base.ClientID))
		if err := src.SendMessage(context, data, true); err != nil {
			tests.Failed(t, "Should have transmitted %+q message successfully: %s", data, err)
		}
		tests.Passed(t, "Should have transmitted %+q message successfully", data)

		res := src.M.Pop()
		if !bytes.Equal(res, data) {
			tests.Info(t, "Expected: %s", data)
			tests.Info(t, "Received: %s", res)
			tests.Failed(t, "Should received command: %+s", data)
		}
		tests.Passed(t, "Should received %+q command", data)

		destInfo := dest.M.Pop()
		dataM, err := dest.Cx.Parser.Parse(destInfo)
		if err != nil {
			tests.Failed(t, "Should have successfully parsed  response: %+q", err)
		}
		tests.Passed(t, "Should have successfully parsed  response")

		if !bytes.Equal(dataM[0].Command, netd.ConnectResMessage) {
			tests.Failed(t, "Should have received %q  response: %+q", netd.ConnectResMessage, dataM[0].Command)
		}
		tests.Passed(t, "Should have received %q  response", netd.ConnectResMessage)

		if err := dest.SendMessage(context, destInfo, true); err != nil {
			tests.Failed(t, "Should have transmitted %+q message successfully: %s", destInfo, err)
		}
		tests.Passed(t, "Should have transmitted %+q message successfully", destInfo)

		fmt.Printf("Src: %+q\n", src.M)
		fmt.Printf("Dest: %+q\n", dest.M)
	}
}

func testInfo(t *testing.T, src *mocks.MessagePipe, dest *mocks.MessagePipe) {
	t.Logf("\tWhen requesting INFO command")
	{

		defer src.M.Reset()
		defer dest.M.Reset()

		data := netd.WrapResponse(netd.InfoMessage)
		if err := src.SendMessage(context, data, true); err != nil {
			tests.Failed(t, "Should have transmitted %+q message successfully: %s", data, err)
		}
		tests.Passed(t, "Should have transmitted %+q message successfully", data)

		res := src.M.Pop()
		if !bytes.Equal(res, data) {
			tests.Info(t, "Expected: %s", data)
			tests.Info(t, "Received: %s", res)
			tests.Failed(t, "Should received command: %+s", data)
		}
		tests.Passed(t, "Should received %+q command", data)

		if src.ShouldClose {
			tests.Failed(t, "Should still have connection open for transmission")
		}
		tests.Passed(t, "Should still have connection open for transmission")

		destInfo := dest.M.Pop()
		dataM, err := dest.Cx.Parser.Parse(destInfo)
		if err != nil {
			tests.Failed(t, "Should have successfully parsed  response: %+q", err)
		}
		tests.Passed(t, "Should have successfully parsed  response")

		if !bytes.Equal(dataM[0].Command, netd.InfoResMessage) {
			tests.Failed(t, "Should have received %q  response: %+q", netd.InfoResMessage, dataM[0].Command)
		}
		tests.Passed(t, "Should have received %q  response", netd.InfoResMessage)
	}
}

func newConnection() (*mocks.MessagePipe, *mocks.MessagePipe) {
	router := routes.New(tracer)
	handler := &nitro.Nitro{Logger: log, Trace: tracer}

	src := mocks.MockConn("127.0.0.1:5060", handler.HandleFire, router)
	dest := mocks.MockConn("127.0.0.1:8060", handler.HandleFire, router)

	return mocks.Pipe(src, dest, handler)
}
