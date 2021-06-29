
open System
open System.Buffers
open System.IO.Pipelines
open System.Net.Sockets
open System.Text
open System.Threading
open System.Threading.Tasks
open BenchmarkDotNet.Attributes
open FSharp.Control.Tasks.V2.ContextInsensitive 

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
            if client = null then client.Dispose() 
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


[<EntryPoint>]
let main argv =
    //let conn = NATS.Transport.createConnectionPair PipeOptions.Default PipeOptions.Default 
//    let pipe = Pipe(PipeOptions.Default) 
    use cts = new CancellationTokenSource(TimeSpan.FromSeconds(30.0))
//    Task.Run(fun () -> Task.WaitAll(NATS.run pipe cancel.Token)) |> ignore
    use progress = new Progress(cancel = cts.Token)
    let bar = progress.AddBar(70)
    bar.AppendCompleted()
    bar.PrependElapsed() 
    progress.Start()
    
    let task = task {
        while not cts.IsCancellationRequested && bar.Increment() do
            do! Task.Delay(50)
        progress.Stop()
    }

    cts.CancelAfter(TimeSpan.FromSeconds(10.0))
    task.Wait(cts.Token)
    cts.Dispose() 
    printfn "Completed"
    0 
    