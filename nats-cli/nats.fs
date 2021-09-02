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
    open FSharp.Control.Tasks.NonAffine
    open FSharp.Control.Tasks.NonAffine.Unsafe
    open FParsec
    open FsToolkit.ErrorHandling

    
    module Client =
        
        module Parser =
            
            type ProtocolMessage =
                | INFO of result: string
                | MSG  of result: struct (string * string * string option * int * string)
                | PING 
                | PONG 
                | OK
                | ERR of msg: string
                
            type internal MsgArgs =
                | Short of int
                | Long of string * int 
            
            let internal INFO = pstring "INFO" .>> spaces1 >>. restOfLine false .>> newline |>> INFO 
            
            let internal isAllowed ch = Char.IsLetterOrDigit(ch) || isAnyOf "_." ch
            let internal arg = many1Satisfy isAllowed
            let internal subject = arg .>> spaces1 
            let internal sid = arg .>> spaces1 
            let internal replyTo = arg .>> spaces1 
            let internal numBytes = pint32 
            let internal endMsg = (numBytes |>> Short) <|> (replyTo .>>. numBytes |>> Long)
            let internal toMSG (((subj, sid), args), payload) =
                match args with
                | Short bytes -> MSG struct (subj, sid, None, bytes, payload)
                | Long (reply, bytes) -> MSG struct (subj, sid, Some reply, bytes, payload) 
            
            let internal MSG = pstring "MSG" .>> spaces1 >>. subject .>>. sid .>>. endMsg .>> newline
                               .>>. restOfLine false .>> newline |>> toMSG
            
            let internal PING = pstring "PING" .>> newline >>% PING
            
            let internal PONG = pstring "PONG" .>> newline >>% PONG 
            
            let internal OK = pstring "+OK" .>> newline >>% OK 
            
            let internal ERR = pstring "-ERR" .>> spaces1 >>. restOfLine false .>> newline |>> ERR 
            
            let protocolMessage = choice [ MSG; INFO; PING; PONG; OK; ERR ]
            
            let parseMessage = runParserOnString protocolMessage () String.Empty
        
        module Receiver =
            
            let subscribe (stream: Stream) subject token =
                uunitVtask {
                    let connId = Guid.NewGuid().ToString("N")
                    let msg = $"SUB %s{subject} %s{connId}\r\n"
                    let bytes = Encoding.UTF8.GetBytes(msg)
                    let rom = ReadOnlyMemory(bytes)
                    do! stream.WriteAsync(rom, token)
                }
                
            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
            let write (stream: Stream) (writer: PipeWriter) token =
                uunitVtask {
                    let mutable error = null
                    try 
                        try
                            let mutable writeMore = true
                            while writeMore do 
                                let buffer = writer.GetMemory(1)
                                let! bytesRead = stream.ReadAsync(buffer, token)
                                writeMore <- bytesRead <> 0
                                if writeMore then
                                    writer.Advance(bytesRead)
                                    let! result = writer.FlushAsync(token)
                                    writeMore <- not result.IsCompleted && not result.IsCompleted
                            
                        with ex ->
                            eprintfn $"{ex}"
                            error <- ex

                    finally
                        writer.Complete(error)
                }
            
            type MsgDetails =
                { subject: string 
                  sid: string 
                  replyTo: string option 
                  numBytes: int 
                  payload: string }
            
            [<Struct>] 
            type NatsMessage =
                | Info of json: string
                | Msg of MsgDetails
                | Ping
                | Pong
                | Ok
                | Error of message: string
                
            with
                static member TryCreate(protocolMessage) =
                    match protocolMessage with
                    | Parser.INFO json -> Info json |> Result.Ok
                    | Parser.PING      -> Ping |> Result.Ok
                    | Parser.PONG      -> Pong |> Result.Ok
                    | Parser.OK        -> Ok |> Result.Ok
                    | Parser.ERR msg   -> Error msg |> Result.Ok
                    | Parser.MSG (s, id, r, n, p) ->
                        { subject = s; sid = id; replyTo = r; numBytes = n; payload = p }
                        |> Msg |> Result.Ok 
            
            [<Literal>]
            let internal NewLine = 10uy
            
            let internal messageFound = Event<NatsMessage>()
                    
            exception NATSException of string
            
            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
            let internal findMessages (buffer: ReadOnlySequence<byte>) =
                let reader = SequenceReader(buffer)
                let mutable carry = StringBuilder()
                let mutable line = String.Empty
                let mutable str = String.Empty
                let mutable byteSpan = ReadOnlySpan<byte>()
                let mutable isMSG = false 
                let mutable readMore = true
                while readMore do
                    match reader.TryReadTo(&byteSpan, NewLine, true) with
                    | false ->
                        if isMSG then reader.Rewind(int64 carry.Length + 1L)
                        readMore <- false
                    | true  ->
                        line <- Encoding.UTF8.GetString(byteSpan)
                        str <- if isMSG then
                                   carry.Append(line.AsSpan()).ToString()
                               else
                                   line 
                        match Parser.parseMessage str with
                        | Failure (errStr, _, _) when isMSG ->
                            NATSException($"Invalid NATS message found.  NATS MSG could not be parsed.\n{errStr}")
                            |> raise
                        | Failure _ ->
                            isMSG <- true 
                            carry <- carry.Append(str) 
                        | Success (msg, _, _) ->
                            NatsMessage.TryCreate(msg)
                            |> Result.either messageFound.Trigger (fun t -> eprintfn $"[WARN] %s{t}")
                            carry <- carry.Clear()
                            isMSG <- false

                reader.Position
            
            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>] 
            let read (reader: PipeReader) token =
                uunitVtask {
                    let mutable error = null
                    try 
                        try
                            let mutable readMore = true
                            while readMore do
                                let! result = reader.ReadAsync(token)
                                readMore <- not result.IsCompleted && not result.IsCanceled
                                if not result.Buffer.IsEmpty then
                                    let pos = findMessages result.Buffer
                                    reader.AdvanceTo(pos, result.Buffer.End)
                        with ex -> 
                            error <- ex 
                    finally
                        reader.Complete(error)
                }
 
        
        exception NATSConnectionException of string
        
        let internal openTcp (url: Uri) = 
            vtask {
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

        let Messages = Receiver.messageFound.Publish

        let listen onUrl forSubject cancel =
            uunitVtask {
                let pipeOptions = PipeOptions(readerScheduler = PipeScheduler.Inline,
                                              writerScheduler = PipeScheduler.Inline,
                                              useSynchronizationContext = false)
                let receivePipe = Pipe(pipeOptions)
                
                try 
                    let! client = openTcp onUrl
                    let stream = client.GetStream()
                    
                    do! Receiver.subscribe stream forSubject cancel
                    let outputTask = Receiver.write stream receivePipe.Writer cancel
                    let inputTask = Receiver.read receivePipe.Reader cancel 
                    
                    do! Task.WhenAll(outputTask.AsTask(), inputTask.AsTask())
                    
                with ex ->
                    eprintfn $"{ex.ToString()}"
                    
                return ()
            }
