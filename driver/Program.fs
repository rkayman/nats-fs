module Aspir

open System
open System.Buffers
open System.IO.Pipelines
open System.Net.Sockets
open System.Text
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open FSharp.Control.Tasks.NonAffine 

open NATS
open UiProgress.Console


[<MemoryDiagnoser>]
type PipeBench() =
    let pipe = Pipe(PipeOptions.Default)
    let mutable client : TcpClient = null 
    let mutable cancel : CancellationTokenSource = null
    let mutable isFirstTime = true

    let parseLine (bytes: ReadOnlySequence<byte>) = 
        let msg = Encoding.UTF8.GetString(&bytes).TrimEnd() 
        printfn $"{msg}" 

    [<GlobalSetup>]
    member _.Setup() =
        task {
            let! tcp = Uri(@"nats://localhost:4222") |> Client.openTcp
            if client <> null then client.Dispose() 
            client <- tcp 
            let stream = client.GetStream()
            use cancel = new CancellationTokenSource(TimeSpan.FromSeconds(5.0))
            do! Client.subscribe stream cancel.Token 
        }
        
    [<IterationSetup>]
    member _.IterSetup() =
        if not isFirstTime then cancel.Dispose() 
        cancel <- new CancellationTokenSource(TimeSpan.FromSeconds(30.))
        if isFirstTime then isFirstTime <- false 
        else pipe.Reset()
        
    [<Benchmark(Baseline=true)>]
    member _.RunBothIterative() =
        task {
            let readTask = Client.readLoop pipe cancel.Token parseLine
            let writeTask = Client.writeLoop client pipe cancel.Token
            let tasks: Task[] = [|readTask; writeTask|]
            
            do! Task.WhenAll(tasks) 
        }
        
    [<Benchmark>]
    member _.RunWriterIterReadRec() = 
        task {
            let readTask = Client.readLoopRec pipe cancel.Token parseLine
            let writeTask = Client.writeLoop client pipe cancel.Token
            let tasks: Task[] = [|readTask; writeTask|]
            
            do! Task.WhenAll(tasks) 
        } 
        
    [<Benchmark>]
    member _.RunWriterIterReadRecInner() = 
        task {
            let readTask = Client.readLoopRecInner pipe cancel.Token parseLine
            let writeTask = Client.writeLoop client pipe cancel.Token
            let tasks: Task[] = [|readTask; writeTask|]
            
            do! Task.WhenAll(tasks) 
        } 
        
    [<Benchmark>]
    member _.RunWriterRecReadIter() = 
        task {
            let readTask = Client.readLoop pipe cancel.Token parseLine
            let writeTask = Client.writeLoopRec client pipe cancel.Token
            let tasks: Task[] = [|readTask; writeTask|]
            
            do! Task.WhenAll(tasks) 
        } 
        
    [<Benchmark>]
    member _.RunWriterRecReadRec() = 
        task {
            let readTask = Client.readLoopRec pipe cancel.Token parseLine
            let writeTask = Client.writeLoopRec client pipe cancel.Token
            let tasks: Task[] = [|readTask; writeTask|]
            
            do! Task.WhenAll(tasks) 
        } 
        
    [<Benchmark>]
    member _.RunWriterRecReadRecInner() = 
        task {
            let readTask = Client.readLoopRecInner pipe cancel.Token parseLine
            let writeTask = Client.writeLoopRec client pipe cancel.Token
            let tasks: Task[] = [|readTask; writeTask|]
            
            do! Task.WhenAll(tasks) 
        } 

let run args =
    task {
        let bench = PipeBench()
        do! bench.Setup()
        do bench.IterSetup()
        //do! bench.RunBothIterative()
        do! bench.RunWriterRecReadRec() 
    }

//[<EntryPoint>]
//let main argv =
//    use cts = new CancellationTokenSource()
//    cts.CancelAfter(TimeSpan.FromSeconds(90.0))
//    let task = run argv
//    task.Wait(cts.Token) 
    
//    use cts = new CancellationTokenSource(TimeSpan.FromSeconds(30.0))
//    use progress = new Progress(cancel = cts.Token)
//    let bar = progress.AddBar(70)
//    bar.AppendCompleted()
//    bar.PrependElapsed() 
//    progress.Start()
//    
//    let task = task {
//        while not cts.IsCancellationRequested && bar.Increment() do
//            do! Task.Delay(50)
//        progress.Stop()
//    }
//
//    cts.CancelAfter(TimeSpan.FromSeconds(10.0))
//    task.Wait(cts.Token)
//    cts.Dispose() 
//    printfn "Completed"
//    0 

open System
open System.Threading
open System.Threading.Channels
open FSharp.Control.Tasks.NonAffine

//[<Struct>]
type Rec =
    { id: Guid
      name: string
      time: DateTime }

let makeMsg msgSize =
    { id = Guid.NewGuid()
      name = String('\000', msgSize - sizeof<Guid> - sizeof<DateTime>)
      time = DateTime.Now }
    
let makeOptions cap isReader =
    if cap < 1 then invalidArg (nameof cap) "Capacity of bounded options must be positive integer greater than zero."
    let bco = BoundedChannelOptions(cap)
    bco.SingleReader <- if isReader then not (cap > 1) else true
    bco.SingleWriter <- if not isReader then not (cap > 1) else true
    bco
    
let pubChannel cnt = Channel.CreateBounded<Rec>(makeOptions cnt false)
let subChannels cnt msgs = Array.init cnt (fun _ -> Channel.CreateBounded<Rec>(makeOptions msgs true))

let runSub (log: MailboxProcessor<string>) (id: int) (numMsgs: int) (msgSize: int) token (ch: ChannelReader<_>) =
    task {
        log.Post $"Running subscriber #{id}..."
        //let batch = numMsgs / 10
        let mutable cnt = 0
        let mutable started = DateTime.MaxValue
        let mutable ended = DateTime.MinValue
        while (cnt < numMsgs) do
            let! _ = ch.ReadAsync(token)
            cnt <- cnt + 1
            if cnt = 1 then started <- DateTime.Now
            elif cnt >= numMsgs then ended <- DateTime.Now
            //if 0 = cnt % batch then log.Post $"Subscriber #{id} processed {cnt:N0} messages"
        
        log.Post $"Subscriber #{id} processed {cnt:N0} {msgSize} byte messages in {(ended - started).TotalMilliseconds} ms"
        // TODO: add sample to benchmark -- benchmark.AddSubSample(newSample(numMsgs, msgSize, started, ended, nc))
        return ()
    }

let runPub (log: MailboxProcessor<string>) (id: int) (numMsgs: int) (msgSize: int) token (ch: ChannelWriter<_>) =
    task {
        log.Post $"Running publisher #{id}..."
        let msg = makeMsg msgSize
        
        let started = DateTime.Now
        
        for _ = 1 to numMsgs do
            do! ch.WriteAsync(msg, token)

        ch.Complete()
        log.Post $"Publisher #{id} processed {numMsgs:N0} messages in {(DateTime.Now - started).TotalMilliseconds} ms"
        // TODO: add sample to benchmark -- benchmark.AddPubSample(newSample(numMsgs, msgSize, started, ended, nc))
        return () 
    }

let messagesPerClient numMessages numPubs =
    if numPubs = 0 || numMessages = 0 then Array.create 0 0
    else
        let count = numMessages / numPubs
        let extra = numMessages % numPubs
        Array.init numPubs (fun x -> count + if x < extra then 1 else 0) 
    
let simulate (log: MailboxProcessor<string>) token (numMsgs: int) (pubReader: ChannelReader<_>) (subWriters: ChannelWriter<_>[]) =
    task {
        log.Post "Running simulator..."
        //let batch = numMsgs / 100
        let mutable cnt = 0
        try
            while true do
                cnt <- cnt + 1
                let! msg = pubReader.ReadAsync(token)
                for sw in subWriters do
                    do! sw.WriteAsync(msg, token)
                //if 0 = cnt % batch then log.Post $"{cnt:N0} total published messages sent to subscribers"
        with
        | :? OperationCanceledException -> log.Post "Simulator cancelled"
        | :? ChannelClosedException -> log.Post "Channel has been closed"

        subWriters |> Array.iter (fun w -> w.Complete())
        log.Post "Simulator finished"
    }
    
let logAgent = MailboxProcessor<string>.Start(fun mbox ->
    let rec loop () =
        async {
            let! msg = mbox.Receive()
            match msg with
            | "done" -> return ()
            | _ ->
                printfn $"{msg}"
                return! loop ()
        }
    loop ())

[<EntryPoint>]
let main argv =
    if argv.Length <> 4 then invalidArg (nameof argv) "4 arguments required: pubCnt, subCnt, msgCnt, and msgSize"
    let argn = argv |> Array.map Int32.Parse
    let pubCnt, subCnt, msgCnt, msgSize = argn.[0], argn.[1], argn.[2], argn.[3]
    let pubChannel = pubChannel pubCnt
    let subChannels = subChannels subCnt msgCnt
    let subWriters = subChannels |> Array.map (fun t -> t.Writer)
    let subReaders = subChannels |> Array.map (fun t -> t.Reader)
    use cts = new CancellationTokenSource(TimeSpan.FromSeconds(30.0))
    let tn = simulate logAgent cts.Token msgCnt pubChannel.Reader subWriters |> Async.AwaitTask
    let ts = subReaders
             |> Array.mapi (fun i r -> runSub logAgent (i+1) msgCnt msgSize cts.Token r |> Async.AwaitTask)
    let msgs = messagesPerClient msgCnt pubCnt
    let tp = msgs
             |> Array.mapi (fun i msg -> runPub logAgent (i+1) msg msgSize cts.Token pubChannel.Writer |> Async.AwaitTask)
    [ [|tn|]; ts; tp ]
    |> Array.concat
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore
    printfn "Done"
    0
