namespace Aspir

module Channels =
    
    open System.Threading
    open System.Threading.Channels
    open FSharp.Control.Tasks.NonAffine
    
    let inline (!!) (x: ^a) : ^b = ((^a or ^b) : (static member op_Implicit : ^a -> ^b) x)
        
    let inline (!!>) (ch: ChannelWriter<'a>) (item, token: CancellationToken) =
        task {
            return! ch.WriteAsync(item, token)
        }
    
    let inline (!>) (ch: ChannelWriter<'a>) item = (!!>) ch (item, Unchecked.defaultof<CancellationToken>)
        
    let inline (!!<) (ch: ChannelReader<'a>) (token: CancellationToken) =
        task {
            return! ch.ReadAsync(token)
        }
        
    let inline (!<) (ch: ChannelReader<'a>) = (!!<) ch Unchecked.defaultof<CancellationToken>

module Bench =
    
    open System
    open System.Text
    open System.Threading.Channels
    open FSharp.Data
    open Channels
    
    /// MessagesPerClient divides the number of messages by the number of publishers and tries to
    /// distribute them as evenly as possible
    let messagesPerClient numMessages numPubs =
        if numPubs = 0 || numMessages = 0 then Array.create 0 0
        else
            let count = numMessages / numPubs
            let extra = numMessages % numPubs
            Array.init numPubs (fun x -> count + if x < extra then 1 else 0) 
    
    /// HumanBytes formats bytes as a human readable string
    let humanBytes (bytes: float) si =
        let base', post, pre =
            if si then 1000, "iB", [| "k"; "M"; "G"; "T"; "P"; "E" |]
            else 1024, "B", [| "K"; "M"; "G"; "T"; "P"; "E" |]
        
        if bytes < float(base') then $"%0.2f{bytes} B"
        else
            let exp = int(log bytes / log (float(base')))
            let idx = exp - 1
            let units = pre.[idx] + post
            $"%0.2f{bytes / (pown (float(base'))  exp)} %s{units}" 

    /// Format number for pretty printing using thousands and decimal separators 
    let inline commaFormat n = $"{n:N}"
        
    /// A Sample for a particular client
    type Sample =
        { JobMessageCount: int
          MessageCount: int64
          MessageBytes: int64
          IOBytes: int64
          Start: DateTime
          End: DateTime }
        
        /// Duration that the sample was active
        member this.Duration = this.End - this.Start
        
        /// Seconds that the sample(s) were active    
        member this.Seconds = this.Duration.TotalSeconds
        
        /// Rate of messages in the job per second    
        member this.Rate = int64(float(this.JobMessageCount) / this.Seconds)

        /// Throughput of bytes per second    
        member this.Throughput = float(this.MessageBytes) / this.Seconds
            
        override this.ToString() =
            let rate = commaFormat this.Rate
            let throughput = humanBytes this.Throughput false
            $"{rate} msgs/sec ~ {throughput}/sec"
    
    module Sample =

        open System.Runtime.CompilerServices
            
        /// Create makes a new Sample initialized to default values.
        let create =
            { JobMessageCount = 0
              MessageCount = 0L
              MessageBytes = 0L
              IOBytes = 0L
              Start = DateTime.MaxValue
              End = DateTime.MinValue }

        /// Init creates a new sample using the supplied values.
        let init jobCount msgSize startAt endAt (conn: obj(*NATS.Client*)) =
            { JobMessageCount = jobCount
              Start = startAt
              End = endAt
              MessageBytes = int64 (msgSize * jobCount)
              MessageCount = 0L     // TODO: conn.OutMsgs + conn.InMsgs
              IOBytes = 0L }        // TODO: conn.OutBytes + conn.InBytes }

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let inline internal getRate (s: Sample) = s.Rate
        
        
    /// SampleGroup for a number of samples, the group is a Sample itself aggregating the values of the Samples
    type SampleGroup =
        { Aggregate: Sample ref
          Samples: ResizeArray<Sample> }

        /// HasSamples returns true if the group has samples
        member this.HasSamples = this.Samples.Count > 0
        
        /// MinRate returns the smallest message rate in the SampleGroup
        member this.MinRate() =
            let xs = this.Samples
            if xs.Count = 0 then None
            else
                xs |> Seq.minBy Sample.getRate |> Sample.getRate |> Some
        
        /// MaxRate returns the largest message rate in the SampleGroup
        member this.MaxRate() =
            let xs = this.Samples
            if xs.Count = 0 then None
            else
                xs |> Seq.maxBy Sample.getRate |> Sample.getRate |> Some
        
        /// AverageRate returns the average of all message rates in the SampleGroup
        member this.AverageRate() =
            let sum = this.Samples |> Seq.sumBy Sample.getRate
            let cnt = this.Samples.Count |> int64
            if cnt = 0L then DivideByZeroException("There must be samples in order to compute the average rate") |> raise
            sum / cnt
            
        /// StdDev returns the standard deviation of the message rates in the SampleGroup
        member this.StdDev() =
            let avg = this.AverageRate() |> float
            let sum = this.Samples |> Seq.sumBy (fun t -> pown (float(t.Rate) - avg)  2)
            let var = sum / float(this.Samples.Count)
            sqrt var 

        /// Statistics information of the sample group (min, average, max, and standard deviation)
        member this.Statistics() =
            $"min %s{this.MinRate() |> Option.defaultValue 0L |> commaFormat} | \
              avg %s{this.AverageRate() |> commaFormat} | \
              max %s{this.MaxRate() |> Option.defaultValue 0L |> commaFormat} | \
              stddev %s{this.StdDev() |> commaFormat} msgs"
            
    module SampleGroup =
        
        /// AddSample adds the given sample to the SampleGroup and then uses the new sample to recompute the aggregate
        let addSample (grp: SampleGroup) x =
            let xs = grp.Samples
            xs.Add(x)
            
            match xs.Count with
            | 1 -> grp.Aggregate := x 
            | _ ->
                let agg = !grp.Aggregate
                let jobMsgCnt' = agg.JobMessageCount + x.JobMessageCount
                let msgCnt'    = agg.MessageCount + x.MessageCount
                let msgBytes'  = agg.MessageBytes + x.MessageBytes
                let ioBytes'   = agg.IOBytes + x.IOBytes
                let start'     = min x.Start agg.Start
                let end'       = max x.End agg.End
                grp.Aggregate := { JobMessageCount = jobMsgCnt'
                                   MessageCount    = msgCnt'
                                   MessageBytes    = msgBytes'
                                   IOBytes         = ioBytes'
                                   Start           = start'
                                   End             = end' }

        /// Create is a helper method to make a new SampleGroup 
        let create () =
            { Aggregate = ref Sample.create
              Samples = ResizeArray<_>(100_000) }

            
    let private makeChannel capacity =
        let opt = BoundedChannelOptions(capacity)
        opt.SingleWriter <- false
        opt.SingleReader <- true 
        Channel.CreateBounded<Sample>(opt)

    type private BenchmarkCsv =
        CsvProvider<Sample="#RunID,ClientID,MsgCount,MsgBytes,MsgsPerSec,BytesPerSec,DurationsSecs",
                    Schema="string,string,int64,int64,int64,float,float", HasHeaders=true>
    
    /// Benchmark to hold the various Samples organized by publishers and subscribers
    [<Struct>]
    type Benchmark =
        { Aggregate: Sample ref
          Name: string
          RunID: string
          Pubs: SampleGroup
          Subs: SampleGroup 

          subChannel: Channel<Sample>
          pubChannel: Channel<Sample> }
        
        /// NewBenchmark initializes a Benchmark.  After creating a bench call
        /// AddSubSample/AddPubSample. When done collecting samples, call EndBenchmark
        static member NewBenchmark(name, subCount: int, pubCount: int) =
            { Aggregate = ref Sample.create
              Name = name
              RunID = Guid.NewGuid().ToString("N")
              Pubs = SampleGroup.create ()
              Subs = SampleGroup.create ()
              
              subChannel = makeChannel subCount
              pubChannel = makeChannel pubCount }

        /// CSV generates a csv report of all the samples collected
        member this.CSV() =
            let bm = this
            let subs = this.Subs
            let pubs = this.Pubs
            let rows = ResizeArray<BenchmarkCsv.Row>(subs.Samples.Count + pubs.Samples.Count)
            
            let addSamples pre (samples: ResizeArray<_>) =
                let makeRow i s =
                    BenchmarkCsv.Row(bm.RunID, $"{pre}{i}", s.MessageCount, s.MessageBytes,
                                     s.Rate, s.Throughput, s.Duration.TotalSeconds)
                let arr = samples |> Seq.mapi makeRow
                rows.AddRange(arr)
            
            pubs.Samples |> addSamples "P"
            subs.Samples |> addSamples "S"
                
            let csv = new BenchmarkCsv(rows)
            csv.SaveToString()
            
        /// Close organizes collected samples and calculates aggregates.  After Close(), no more samples can be added.
        member this.Close() =
            this.subChannel.Writer.Complete()
            this.pubChannel.Writer.Complete()

            let addSamples (ch: ChannelReader<_>) (grp: SampleGroup) =
                for _ = 1 to ch.Count do
                    match ch.TryRead() with
                    | false, _ -> ()
                    | true, s  -> SampleGroup.addSample grp s 

            addSamples this.subChannel.Reader this.Subs
            addSamples this.pubChannel.Reader this.Pubs 
            
            let agg = !this.Aggregate
            let subs, pubs = !this.Subs.Aggregate, !this.Pubs.Aggregate

            let agg' = { agg with MessageBytes = pubs.MessageBytes + subs.MessageBytes
                                  IOBytes = pubs.IOBytes + subs.IOBytes
                                  MessageCount = pubs.MessageCount + subs.MessageCount
                                  JobMessageCount = pubs.JobMessageCount + subs.JobMessageCount }
            
            this.Aggregate := match this.Subs.HasSamples, this.Pubs.HasSamples with
                              | true, true ->
                                  { agg' with Start = min agg.Start subs.Start |> min pubs.Start
                                              End   = max agg.End subs.End |> max pubs.End }
                              | true, false ->
                                  { agg' with Start = subs.Start; End = subs.End }
                              | false, true ->
                                  { agg' with Start = pubs.Start; End = pubs.End }
                              | false, false -> agg'

        /// Report returns a human readable report of the samples taken in the benchmark
        member this.Report() =
            let sampleReport ident hdr (grp: SampleGroup) buf =
                Printf.bprintf buf $"{ident}{hdr} stats: {!grp.Aggregate}\n"
                grp.Samples
                |> Seq.iteri (fun i s -> Printf.bprintf buf $"{ident} [%d{i + 1}] {s} (%d{s.JobMessageCount})\n")
                buf
                
            let buf = StringBuilder(1024)
            match this.Pubs.HasSamples, this.Subs.HasSamples with
            | false, false ->
                "No publisher or subscribers. Nothing to report."
            | true, false  ->
                buf |> sampleReport String.Empty "Pub" this.Pubs |> (sprintf "%A")
            | false, true  ->
                buf |> sampleReport String.Empty "Sub" this.Subs |> (sprintf "%A") 
            | true, true   ->
                Printf.bprintf buf $"{this.Name} Pub/Sub stats: {!this.Aggregate}\n"
                buf |> sampleReport " " "Pub" this.Pubs
                    |> sampleReport " " "Sub" this.Subs
                    |> (sprintf "%A")

    module Benchmark =
        
        let addSample (ch: Channel<Sample>) s =
            let t = !> (!! ch) s
            if t.Wait(1_000) then () else TimeoutException("Unable to add sample to channel") |> raise

    type Benchmark with 
            
        member this.AddSubSample(s: Sample) = Benchmark.addSample this.subChannel s 
            
        member this.AddPubSample(s: Sample) = Benchmark.addSample this.pubChannel s 
