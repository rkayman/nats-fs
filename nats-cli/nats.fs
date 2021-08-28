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
                | INFO of result: string
                | MSG of result: struct (string * string * string option * int)
                | MSGP of payload: string
                | PING 
                | PONG 
                | OK
                | ERR of msg: string
                
            type private MsgArgs =
                | Short of int
                | Long of string * int 
            
            let private INFO = pstring "INFO" .>> spaces1 >>. restOfLine false .>> newline |>> INFO 
            
            let private isAllowed ch = Char.IsLetterOrDigit(ch) || isAnyOf "_." ch
            let private arg = many1Satisfy isAllowed
            let private subject = arg .>> spaces1 
            let private sid = arg .>> spaces1 
            let private replyTo = arg .>> spaces1 
            let private numBytes = pint32 
            let private endMsg = (numBytes |>> Short) <|> (replyTo .>>. numBytes |>> Long)
            let private toMSG ((subj, sid), args) =
                match args with
                | Short bytes -> MSG struct (subj, sid, None, bytes)
                | Long (reply, bytes) -> MSG struct (subj, sid, Some reply, bytes) 
            
            let private MSG = pstring "MSG" .>> spaces1 >>. subject .>>. sid .>>. endMsg .>> newline |>> toMSG 
            
            let private Payload numBytes = anyString numBytes .>> newline |>> MSGP
            
            let private PING = pstring "PING" .>> newline >>% PING
            
            let private PONG = pstring "PONG" .>> newline >>% PONG 
            
            let private OK = pstring "+OK" .>> newline >>% OK 
            
            let private ERR = pstring "-ERR" .>> spaces1 >>. restOfLine false .>> newline |>> ERR 
            
            let private protocolMessage = choice [ MSG; INFO; PING; PONG; OK; ERR ]
            
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
                            error <- ex

                    finally
                        writer.Complete(error)
                }
            
            let private NewLine = byte('\n')
            
            [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
            let private getNextNewLinePosition buf = buf.PositionOf(NewLine) |> Option.ofNullable
                
            let internal lineFound = Event<string>()
            
            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
            let rec private processBuffer (buf: ReadOnlySequence<byte>) =
                match getNextNewLinePosition buf with
                | None -> buf
                | Some pos ->
                    let seq = buf.Slice(0, pos)
                    let line = Encoding.UTF8.GetString(&seq)
                    lineFound.Trigger(line)
                    buf.Slice(buf.GetPosition(1L, pos)) |> processBuffer

            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>] 
            let read (reader: PipeReader) token =
                task {
                    let mutable error = null
                    try 
                        try
                            let mutable buf = Unchecked.defaultof<ReadOnlySequence<byte>> 
                            let mutable readMore = true
                            while readMore do
                                let! result = reader.ReadAsync(token)
                                readMore <- not result.IsCompleted && not result.IsCanceled
                                if not result.Buffer.IsEmpty then
                                    buf <- processBuffer result.Buffer 
                                    reader.AdvanceTo(buf.Start, buf.End)
                        with ex -> 
                            error <- ex 
                    finally
                        reader.Complete(error)
                } :> Task
            
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
                static member TryCreate(protocolMessages) =
                    match protocolMessages with
                    | [] -> invalidOp "No NATS protocol messages to process."
                    | x::[] ->
                        match x with
                        | Parser.INFO json -> Info json |> Result.Ok
                        | Parser.PING      -> Ping |> Result.Ok
                        | Parser.PONG      -> Pong |> Result.Ok
                        | Parser.OK        -> Ok |> Result.Ok
                        | Parser.ERR msg   -> Error msg |> Result.Ok
                        | _                -> Result.Error "Received one protocol message, but expected two protocol messages"
                    | x::y::[] ->
                        match x, y with
                        | Parser.MSGP p, Parser.MSG (subj, id, reply, n) ->
                            { subject = subj
                              sid = id
                              replyTo = reply
                              numBytes = n
                              payload = p }
                            |> Msg |> Result.Ok
                        | _ ->
                            Result.Error "Unknown NATS protocol messages found."
                    | _ ->
                        Result.Error "Unexpected number (i.e. more than 2) NATS protocol messages found."
            
            let internal messageFound = Event<NatsMessage>()
                
            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
            let private parseNextMessage (buf: ReadOnlySequence<byte>) parser =
                let rec processBuffer pmsgs pf buf =
                    match getNextNewLinePosition buf with
                    | None     -> buf, pf 
                    | Some pos ->
                        let loop p = buf.Slice(buf.GetPosition(1L, p)) |> processBuffer [] Parser.parseMessage 
                        let seq = buf.Slice(0, pos)
                        let line = Encoding.UTF8.GetString(&seq)
                        match pf line with
                        | Success (res, _, _) ->
                            match res with
                            | Parser.MSG (_, _, _, bytes) ->
                                buf.Slice(buf.GetPosition(1L, pos))
                                |> processBuffer [res] (Parser.parsePayload bytes)
                            | Parser.MSGP _ ->
                                NatsMessage.TryCreate(res::pmsgs)
                                |> Result.either messageFound.Trigger (fun t -> eprintfn $"[WARN] %s{t}") 
                                loop pos 
                            | _ ->
                                NatsMessage.TryCreate([res])
                                |> Result.either messageFound.Trigger (fun t -> eprintfn $"[WARN] %s{t}") 
                                loop pos 
                        | Failure (errStr, _, _) ->
                            eprintfn $"[WARN] %s{errStr}"
                            loop pos
                processBuffer [] parser buf 

            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>] 
            let readMessages (reader: PipeReader) token =
                task {
                    let mutable error = null
                    try 
                        try
                            let mutable buf = Unchecked.defaultof<ReadOnlySequence<byte>>
                            let mutable parser = Parser.parseMessage
                            let mutable foo = (Unchecked.defaultof<ReadOnlySequence<byte>>, Parser.parseMessage)
                            let mutable readMore = true
                            while readMore do
                                let! result = reader.ReadAsync(token)
                                readMore <- not result.IsCompleted && not result.IsCanceled
                                if not result.Buffer.IsEmpty then
                                    foo <- parseNextMessage result.Buffer (snd foo) 
                                    reader.AdvanceTo((fst foo).Start, (fst foo).End)
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

        let Lines = Receiver.lineFound.Publish
        let Messages = Receiver.messageFound.Publish

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
                    //let inputTask  = Receiver.read receivePipe.Reader cancel
                    let inputTask = Receiver.readMessages receivePipe.Reader cancel 
                    
                    do! Task.WhenAll(outputTask, inputTask)
                    
                with ex ->
                    eprintfn $"{ex.ToString()}"
                    
                return ()
            } :> Task
