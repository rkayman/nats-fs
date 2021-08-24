namespace NATS 

    open System
    open System.Buffers
    open System.IO
    open System.IO.Pipelines
    open System.Net.Sockets
    open System.Runtime.CompilerServices
    open System.Text 
    open System.Threading
    open System.Threading.Tasks
    open FSharp.Control.Tasks.V2.ContextInsensitive
    open FParsec
    open FsToolkit.ErrorHandling

    
    module Client =
        
        module Parser =
            
            type ProtocolMessage =
                | Info of result: string
                | Msg of result: (string * string) * int
                | Payload of result: string
                | Ping 
                | Pong 
                | Ok
                | Err of msg: string 
            
            let INFO = pstring "INFO" .>> spaces1 >>. restOfLine false |>> Info 
            
            let isAllowed ch = Char.IsLetterOrDigit(ch) || isAnyOf "_." ch
            let arg = manySatisfy isAllowed
            let spcstr = pstring " "
            
            let MSG = pstring "MSG" .>> spaces1 .>>. stringsSepBy1 arg spcstr .>>. pint32 .>> newline |>> Msg
            
            let Payload numBytes = anyString numBytes .>> newline |>> Payload
            
            let PING = pstring "PING" .>> newline >>% Ping
            
            let PONG = pstring "PONG" .>> newline >>% Pong
            
            let OK = pstring "+OK" .>> newline >>% Ok
            
            let ERR = pstring "-ERR" .>> spaces1 >>. restOfLine false .>> newline |>> Err 
            
            let protocolMessage = choice [ MSG
                                           INFO
                                           PING
                                           PONG
                                           OK
                                           ERR ]
            
            let parseMessage = runParserOnString protocolMessage () String.Empty
            
            let parsePayload n = runParserOnString (Payload n) () String.Empty 
        
        module Receiver = 
            
            let subscribe (stream: Stream) subject token =
                task {
                    let connId = Guid.NewGuid().ToString("N")
                    let msg = $"SUB %s{subject} %s{connId}\r\n"
                    let bytes = Encoding.UTF8.GetBytes(msg)
                    let rom = ReadOnlyMemory(bytes)
                    do! stream.WriteAsync(rom, token)
                } :> Task
                
            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
            let write (stream: Stream) (writer: PipeWriter) token =
                task {
                    let mutable error = null
                    try 
                        try
                            let writeMore = ref true
                            while !writeMore do 
                                let buffer = writer.GetMemory(1)
                                let! bytesRead = stream.ReadAsync(buffer, token)
                                writeMore := bytesRead <> 0
                                if ! writeMore then
                                    writer.Advance(bytesRead)
                                    let! result = writer.FlushAsync(token)
                                    writeMore := not result.IsCompleted && not result.IsCompleted
                            
                        with ex ->
                            error <- ex

                    finally
                        writer.Complete(error)
                }
                
            let private lineFound = Event<string>()
            let Lines = lineFound.Publish
            
            let private NewLine = byte('\n')
            
            [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
            let private getNextNewLinePosition buf = buf.PositionOf(NewLine) |> Option.ofNullable
            
            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
            let rec private processBuffer (buf: ReadOnlySequence<byte> byref) =
                match getNextNewLinePosition buf with
                | None -> buf
                | Some pos ->
                    let seq = buf.Slice(0, pos)
                    let line = Encoding.UTF8.GetString(&seq)
                    lineFound.Trigger(line)
                    buf <- buf.Slice(buf.GetPosition(1L, pos))
                    processBuffer &buf

            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>] 
            let read (reader: PipeReader) token =
                task {
                    let mutable error = null
                    try 
                        try
                            //let buf = ref Unchecked.defaultof<ReadOnlySequence<byte>>
                            let mutable buf = Unchecked.defaultof<ReadOnlySequence<byte>> 
                            let readMore = ref true
                            while !readMore do
                                let! result = reader.ReadAsync(token)
                                readMore := not result.IsCompleted && not result.IsCanceled
                                if not result.Buffer.IsEmpty then
                                    buf <- result.Buffer
                                    buf <- processBuffer &buf 
                                    reader.AdvanceTo(buf.Start, buf.End)
                        with ex -> 
                            error <- ex 
                    finally
                        reader.Complete(error)
                } :> Task
 
        
        exception NATSConnectionException of string
        
        let private openTcp (url: Uri) = 
            task {
                if url.Scheme.ToLowerInvariant() <> "nats" then
                    raise (NATSConnectionException
                               $"Expected NATS scheme.  Unable to connect to unknown scheme ({url.Scheme})")

                let addressFamily, dualMode = if Socket.OSSupportsIPv6 
                                              then AddressFamily.InterNetworkV6, true 
                                              else AddressFamily.InterNetwork, false 

                let client = new TcpClient(addressFamily) 
                client.Client.DualMode <- dualMode
                client.ReceiveBufferSize <- 64 * 1024
                client.SendBufferSize <- 32 * 1024 

                let server = if url.IsDefaultPort then UriBuilder(url.Scheme, url.Host, 4222).Uri else url 

                use cancel = new CancellationTokenSource(2000)
                do! client.ConnectAsync(server.Host, server.Port, cancel.Token)
                return client
            }

        let listen onUrl forSubject cancel =
            task {
                let pipeOptions = PipeOptions(readerScheduler = PipeScheduler.Inline,
                                              writerScheduler = PipeScheduler.Inline,
                                              useSynchronizationContext = false)
                let receivePipe = Pipe(pipeOptions)
                
                try 
                    let! client = openTcp onUrl
                    let stream = client.GetStream()
                    
                    do! Receiver.subscribe stream forSubject cancel
                    let outputTask = Receiver.write stream receivePipe.Writer cancel
                    let inputTask  = Receiver.read receivePipe.Reader cancel 
                    
                    do! Task.WhenAll(outputTask, inputTask)
                    
                with ex ->
                    eprintfn $"{ex.ToString()}"
                    
                return ()
            } :> Task
