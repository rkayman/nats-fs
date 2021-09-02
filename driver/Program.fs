namespace Aspir

module Driver =

    open System
    open System.Runtime.CompilerServices
    open System.Threading
    open System.Threading.Tasks
    open FSharp.Control.Reactive.Observable
    open FSharp.Control.Reactive.Scheduler
    open Aspir.Bench
    

    open NATS
    open NATS.Client.Receiver
    open Utilities.UiLive
    open UiProgress.Console

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

//    let messagesPerClient numMessages numPubs =
//        if numPubs = 0 || numMessages = 0 then Array.create 0 0
//        else
//            let count = numMessages / numPubs
//            let extra = numMessages % numPubs
//            Array.init numPubs (fun x -> count + if x < extra then 1 else 0)
        
    [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
    let logAgent (writer: Writer) = MailboxProcessor.Start (fun inbox ->
        let rec loop (w: Writer) cnt = async {
            match! inbox.Receive() with
            | NatsMessage.Info json ->
                w.Write($"[INFO] %s{json}")
                return! loop w cnt 
            | NatsMessage.Error err ->
                w.Write($"[ERROR] %s{err}")
                return! loop w cnt 
            | NatsMessage.Msg details ->
                w.Write($"[#{cnt}] Received on \"%s{details.subject}\"\n%s{details.payload}\n")
                return! loop w (cnt + 1)
            | x ->
                w.Write($"[{x}]")
                return! loop w cnt 
        }
        loop writer 1)
    
    let inline logMsg (agent: MailboxProcessor<NatsMessage>) msg = agent.Post(msg)
    
    let mutable private msgCnt = 0
    
    [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
    let logMessage (writer: Writer) msg =
        match msg with
        | NatsMessage.Info json    -> $"[INFO] %s{json}"
        | NatsMessage.Error errStr -> $"[ERROR] %s{errStr}"
        | NatsMessage.Msg details  -> 
            $"[#{Interlocked.Increment(&msgCnt)}] Received on \"%s{details.subject}\"\n%s{details.payload}\n"
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

        use cancel = Console.CancelKeyPress.Subscribe(fun _ -> cts.Cancel())
        use logger = logAgent writer 

        try
            try 
                let timeFormat = "HH:mm:ss"
                eprintfn $"{DateTime.Now.ToString(timeFormat)} Subscribing on {subject}"
                use sub = Client.Messages.Subscribe(logMessage writer)
                let server = Uri("nats://localhost")
                let client = Client.listen server subject cts.Token

                Task.WhenAll(client.AsTask()).Wait()
                
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
