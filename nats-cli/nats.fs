open System
open System.Buffers
open System.Collections.Generic
open System.IO
open System.IO.Pipelines
open System.Net
open System.Net.Sockets
open System.Security.Cryptography.X509Certificates
open System.Text 
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive


module NATS = 
    module Transport = 

        type DuplexPipe(input: PipeReader, output: PipeWriter) = 
            interface IDuplexPipe with 
                member this.Input with get() = input 
                member this.Output with get() = output 

        let createConnectionPair inputOptions outputOptions = 
            let input  = Pipe(inputOptions)
            let output = Pipe(outputOptions)

            let transportToApplication = DuplexPipe(output.Reader, input.Writer) 
            let applicationToTransport = DuplexPipe(input.Reader, output.Writer) 

            struct {| Transport = applicationToTransport; Application = transportToApplication|} 


exception NATSConnectionException of string 

let openTcp (url: Uri) = 
    task {
        if url.Scheme.ToLowerInvariant() <> "nats" then raise (NATSConnectionException $"Unable to connect to unknown scheme ({url.Scheme})")

        let addressFamily, dualMode = if Socket.OSSupportsIPv6 
                                      then AddressFamily.InterNetworkV6, true 
                                      else AddressFamily.InterNetwork, false 

        let client = new TcpClient(addressFamily) 
        client.Client.DualMode <- dualMode 

        let server = if url.IsDefaultPort then UriBuilder(url.Scheme, url.Host, 4222).Uri else url 

        use cancel = new CancellationTokenSource(2000)
        do! client.ConnectAsync(server.Host, server.Port, cancel.Token) 
        if cancel.IsCancellationRequested then raise (NATSConnectionException "timeout")
        client.NoDelay <- false 
        client.ReceiveBufferSize <- 64 * 1024 
        client.SendBufferSize <- 32 * 1024 
        return client
    }

let unitTask = ValueTask.FromResult(()) 

let subscribe (stream: NetworkStream) token = 
    task {
        let msg = "SUB > 100\r\n" 
        let bytes = Encoding.UTF8.GetBytes(msg) 
        let rom = ReadOnlyMemory(bytes)
        do! stream.WriteAsync(rom, token) 
    }

let writeLoopRec (pipe: Pipe) token = 
    task {
        use! client = Uri(@"nats://localhost:4222") |> openTcp 
        let stream = client.GetStream() 
        do! subscribe stream token 

        let rec writeMore (stream: NetworkStream) (writer: PipeWriter) token = 
            task {
                try 
                    let buffer = writer.GetMemory(1) 
                    match! stream.ReadAsync(buffer, token) with 
                    | 0 -> return! ValueTask.FromResult(false) 
                    | bytesRead -> 
                        writer.Advance(bytesRead) 
                        match! writer.FlushAsync(token) with 
                        | x when x.IsCanceled || x.IsCompleted -> return! ValueTask.FromResult(false) 
                        | _ -> return! writeMore stream writer token 

                with ex as Exception -> 
                        do! writer.CompleteAsync(ex) 
                        return! ValueTask.FromResult(true) 
            }
        let! isComplete = writeMore stream pipe.Writer token 
        if not isComplete then do! pipe.Writer.CompleteAsync() 
    }

let writeLoop (pipe: Pipe) token = 
    task {
        use! client = Uri(@"nats://localhost:4222") |> openTcp 
        let stream = client.GetStream() 
        do! subscribe stream token 

        let writer = pipe.Writer 
        let mutable error = null 
        try 
            let mutable more = true 
            while more do 
                let buffer = writer.GetMemory(1) 
                let! bytesRead = stream.ReadAsync(buffer, token) 
                if bytesRead = 0 then more <- false 
                else 
                    writer.Advance(bytesRead) 
                    let! flush = writer.FlushAsync(token) 
                    if flush.IsCanceled || flush.IsCompleted then more <- false 
            
        with ex as Exception -> 
                error <- ex

        do! writer.CompleteAsync(error) 
    }

let readLoopRec (pipe: Pipe) token processLine = 
    task { 
        let rec processBuffer buf = 
            match buf.PositionOf((byte)'\n') |> Option.ofNullable with 
            | None -> buf
            | Some pos -> 
                processLine(buf.Slice(0, pos)) 
                processBuffer (buf.Slice(buf.GetPosition(1L, pos))) 

        let rec readMore (reader: PipeReader) token = 
            task {
                try 
                    match! reader.ReadAsync(token) with 
                    | x when x.IsCompleted || x.IsCanceled -> return! ValueTask.FromResult(false)
                    | result -> 
                        let buffer = processBuffer result.Buffer 
                        reader.AdvanceTo(buffer.Start, buffer.End)
                        return! readMore reader token

                with ex as Exception -> 
                        do! reader.CompleteAsync(ex) 
                        return! ValueTask.FromResult(true) 
            }
        let! isComplete = readMore pipe.Reader token 
        if not isComplete then do! pipe.Reader.CompleteAsync() 
    }

let readLoopRecInner (pipe: Pipe) token processLine = 
    task { 
        let reader = pipe.Reader 

        let mutable error = null
        try 
            let mutable readMore = true 
            while readMore do 
                let! result = reader.ReadAsync(token) 

                let rec processBuffer buf = 
                    match buf.PositionOf((byte)'\n') |> Option.ofNullable with 
                    | None -> buf
                    | Some pos -> 
                        processLine(buf.Slice(0, pos)) 
                        processBuffer (buf.Slice(buf.GetPosition(1L, pos))) 
                let buffer = processBuffer result.Buffer 

                reader.AdvanceTo(buffer.Start, buffer.End) 

                readMore <- not (result.IsCompleted || result.IsCanceled)

        with ex as Exception -> 
                error <- ex 

        do! reader.CompleteAsync(error)
    }

let readLoop (pipe: Pipe) token processLine = 
    task { 
        let reader = pipe.Reader 

        let mutable error = null
        try 
            let mutable readMore = true 
            while readMore do 
                let! result = reader.ReadAsync(token) 

                let mutable buffer = result.Buffer 
                let mutable processMore = true 
                while processMore do 
                    let position = buffer.PositionOf((byte)'\n')
                    if not position.HasValue 
                    then processMore <- false 
                    else 
                        processLine(buffer.Slice(0, position.Value)) 
                        buffer <- buffer.Slice(buffer.GetPosition(1L, position.Value)) 

                reader.AdvanceTo(buffer.Start, buffer.End) 

                readMore <- not (result.IsCompleted || result.IsCanceled)

        with ex as Exception -> 
                error <- ex 

        do! reader.CompleteAsync(error)
    }

let parseLine (bytes: ReadOnlySequence<byte>) = 
    let msg = Encoding.UTF8.GetString(&bytes).TrimEnd() 
    printfn $"{msg}" 

let run pipe token = 
    task {
        let readTask = readLoop pipe token parseLine
        let writeTask = writeLoop pipe token 
        let tasks : Task[] = [|readTask; writeTask|]

        do! Task.WhenAll(tasks) 
    } 

//let conn = NATS.Transport.createConnectionPair PipeOptions.Default PipeOptions.Default 
let reader = Pipe(PipeOptions.Default) 

reader.Reset() 
let cancel = new CancellationTokenSource()
Task.Run(fun () -> Task.WaitAll(run reader cancel.Token))

cancel.Cancel()
cancel.IsCancellationRequested
