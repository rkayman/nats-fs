namespace Aspir

module Driver =

    open System
    open System.Buffers
    open System.Collections.Concurrent
    open System.IO.Pipelines
    open System.Net.Sockets
    open System.Runtime.CompilerServices
    open System.Text
    open System.Threading
    open System.Threading.Channels
    open System.Threading.Tasks
    open FSharp.Control.Tasks.V2.ContextInsensitive
    open BenchmarkDotNet.Attributes
    open BenchmarkDotNet.Running
    open Aspir.Bench
    

    open NATS
    open Utilities.UiLive
    open UiProgress.Console

(*
    [<MemoryDiagnoser>]
    type PipeBench() =
        let mutable cancel : CancellationTokenSource = null
        let mutable isFirstTime = true
            
        let mutable output = Task.CompletedTask 
        let mutable log = null
        let mutable sub = null
        let url = Uri(@"nats://localhost:4222")

        let mutable msgCnt = 0
        let logger (ch: ChannelWriter<string>) msg =
            task {
                Interlocked.Increment(&msgCnt) |> ignore
                let msg' = $"[#{msgCnt:D6}] %s{msg}"
                let rec loop item = 
                    task {
                        if ch.TryWrite(item) then return ()
                        else 
                            match! ch.WaitToWriteAsync() with
                            | false -> return ()
                            | true  ->
                                if ch.TryWrite(item) then return ()
                                else return! loop item 
                    }
                return! loop msg' 
            } :> Task
                            
        let printer (ch: ChannelReader<string>) token =
            task {
                let msg = ref Unchecked.defaultof<string>
                let mutable more = true
                while more do
                    match! ch.WaitToReadAsync(token) with
                    | false ->
                        more <- false 
                    | true  ->
                        while ch.TryRead(msg) do ()  //eprintfn $"{!msg}"
            } :> Task

        
        [<GlobalSetup>]
        member _.Setup() =
            let opts = UnboundedChannelOptions()
            opts.SingleReader <- true
            log <- Channel.CreateUnbounded<string>(opts)
            sub <- Client.Lines.Subscribe(fun line -> (logger log.Writer line).Wait())
            
        [<GlobalCleanup>]
        member _.Cleanup() =
            sub.Dispose()
            
        [<IterationSetup>]
        member _.IterSetup() =
            if not isFirstTime then cancel.Dispose() 
            cancel <- new CancellationTokenSource(TimeSpan.FromSeconds(30.))
            if isFirstTime then isFirstTime <- false
            try
                output <- printer log.Reader cancel.Token
            with _ -> ()
            
            
        [<Benchmark(Baseline=true)>]
        member _.RunListen() =
            try
                let task = Client.listen url "test" cancel.Token
                Task.WaitAll(task, output)
            with _ -> ()
            
                    
    [<EntryPoint>]
    let main argv =
        BenchmarkRunner.Run<PipeBench>()
        |> printfn "%A"
        0
*)
   
//    module Bench = 
//        open System.Threading.Channels
//        open Aspir.Channels
//            
//        [<Struct>]
//        type Rec =
//            { id: Guid
//              name: string
//              time: DateTime }
//
//        let makeMsg msgSize =
//            { id = Guid.NewGuid()
//              name = String('\000', msgSize - sizeof<Guid> - sizeof<DateTime>)
//              time = DateTime.Now }
            
//        let runPub (id: int) (progress: Progress) (numMsgs: int) (msgSize: int) (ch: ChannelQ<_>) (bm: Benchmark) token (log: string -> Task<unit>) =
//            task {
//                let msg = makeMsg msgSize
//                
//                let bar = progress.AddBar(numMsgs)
//                bar.AppendCompleted()
//                bar.PrependElapsed()
//                
//                let started = DateTime.Now
//                let mutable cnt = 0
//                try 
//                    while cnt < numMsgs do
//                        cnt <- cnt + 1
//                        bar.Increment() |> ignore
//                        ch.Add(msg, token)
//                        
//                    ch.CompleteAdding()
//                with
//                | :? OperationCanceledException -> do! log $"Argh! Publisher #{id:N} cancelled"
//                | :? ObjectDisposedException -> do! log $"Yikes, Publisher #{id:N} is disposed"
//                | :? InvalidOperationException as ex  -> do! log $"Ouch! {ex.ToString()}"
//                
//                Sample.init numMsgs msgSize started DateTime.Now null
//                |> Benchmark.addSample bm.pubChannel 
//            }


    let messagesPerClient numMessages numPubs =
        if numPubs = 0 || numMessages = 0 then Array.create 0 0
        else
            let count = numMessages / numPubs
            let extra = numMessages % numPubs
            Array.init numPubs (fun x -> count + if x < extra then 1 else 0)
        
    let mutable private msgCnt = 0
    
    [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
    let logMessage (writer: Writer) msg =
        match msg with
        | Client.Receiver.NatsMessage.Info json    -> $"[INFO] %s{json}"
        | Client.Receiver.NatsMessage.Error errStr -> $"[ERROR] %s{errStr}"
        | Client.Receiver.NatsMessage.Msg details  -> 
            let cnt = Interlocked.Increment(&msgCnt)
            $"[#{cnt}] Received on \"%s{details.subject}\"\n%s{details.payload}\n"
        | x -> $"[{x}]"
        |> writer.Write
    
    [<EntryPoint>]
    let main argv =
        let expectedArgs = 2
        if Array.length argv <> expectedArgs then
            failwith $"{expectedArgs} arguments required: <number of subscribers> <subject on which to listen>"

        let numSub = argv.[0] |> Int32.Parse
        let subject = argv.[1] 

        use cts = new CancellationTokenSource()
        use writer = new Writer(cancel = cts.Token)
        writer.Start()

        use cancel = Console.CancelKeyPress.Subscribe(fun obs ->
            obs.Cancel <- false
            cts.Cancel())

        try
            try 
                let timeFormat = "HH:mm:ss"
                eprintfn $"{DateTime.Now.ToString(timeFormat)} Subscribing on {subject}"
                use sub = Client.Messages.Subscribe(logMessage writer)
                let server = Uri("nats://localhost")
                let client = Client.listen server subject cts.Token

                Task.WhenAll(client).Wait()
                
            with
            | :? AggregateException as ae ->
                match ae.InnerException with
                | :? TaskCanceledException -> ()
                | ex -> eprintfn $"{ex.ToString()}"
            | ex -> eprintfn $"{ex.ToString()}"

        finally
            writer.Flush()
            writer.Stop()
            cancel.Dispose() 

        0
