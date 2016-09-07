package parsers

// // Parse parses the provided slice of bytes recieved from the TCPProvider read loop and using the internal
// // parser to retrieve the messages and the appropriate actions to take.
// func (rl *TCPProvider) parse(context interface{}, data []byte) error {
//  rl.Config.Log.Log(context, "parse", "Started  :  Connection [%s] :  Data{%+q}", rl.Addr, data)

//  messages, err := rl.parser.Parse(data)
//  if err != nil {
//    rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection[%s] : Data{%+q}", rl.Addr)
//    return err
//  }

//  rl.Config.Trace.Begin(context, []byte("parse"))
//  rl.Config.Trace.Trace(context, []byte(fmt.Sprintf("%+q\n", messages)))
//  rl.Config.Trace.End(context, []byte("parse"))

//  for _, message := range messages {
//    cmd := bytes.ToUpper(message.Command)
//    dataLen := len(message.Data)

//    switch {
//    case bytes.Equal(cmd, connect):
//      info, err := json.Marshal(rl.Connection.ServerInfo)
//      if err != nil {
//        rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
//        return err
//      }

//      rl.SendResponse(context, info, true)
//    case bytes.Equal(cmd, info):
//      info, err := json.Marshal(rl.BaseInfo())
//      if err != nil {
//        rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
//        return err
//      }

//      rl.SendResponse(context, info, true)
//    case bytes.Equal(cmd, cluster):
//      if dataLen != 2 {
//        err := errors.New("Invalid Cluster Data, expected {CLUSTER|ADDR|PORT}")
//        rl.SendError(context, invalidClusterInfo, true)
//        rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
//        return err
//      }

//      addr := string(message.Data[0])
//      port, err := strconv.Atoi(string(message.Data[1]))
//      if err != nil {
//        rl.SendError(context, fmt.Errorf("Port is not a int: "+err.Error()), true)
//        rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
//        return err
//      }

//      if err := rl.Connections.NewClusterFromAddr(context, addr, port); err != nil {
//        rl.SendError(context, fmt.Errorf("New Cluster Connect failed: "+err.Error()), true)
//        rl.Config.Log.Error(context, "parse", err, "Completed  :  Connection [%s]", rl.Addr)
//        return err
//      }

//      return rl.SendResponse(context, okMessage, true)
//    case bytes.Equal(cmd, sub):

//    case bytes.Equal(cmd, unsub):
//    case bytes.Equal(cmd, msgBegin):
//    case bytes.Equal(cmd, msgEnd):
//    }
//  }

//  rl.Config.Log.Log(context, "parse", "Completed  :  Connection [%s]", rl.Addr)
//  return nil
// }
