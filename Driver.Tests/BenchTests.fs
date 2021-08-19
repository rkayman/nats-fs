namespace Aspir.Driver

module Bench =

    module TestCases = 

        open System 
        open Expecto
        open Aspir.Bench

        let MsgSize = 8
        let Million = 1000 * 1000

        let baseTime = DateTime.Now

        let createSampleWithMillionMessagesPerSecond seconds =
            let messages = Million * seconds
            let started = baseTime
            let ended = started.AddSeconds(seconds |> float)
            
            let s = Sample.init messages MsgSize started ended null
            let msgBytes = messages * MsgSize 
            {s with MessageCount = int64(messages); MessageBytes = int64(msgBytes); IOBytes = int64(msgBytes) }

        let makeBench subs pubs =
            let bm = Benchmark.NewBenchmark("test", subs, pubs)
            for _ = 1 to subs do
                createSampleWithMillionMessagesPerSecond 1 |> bm.AddSubSample
            for _ = 1 to pubs do
                createSampleWithMillionMessagesPerSecond 1 |> bm.AddPubSample
            bm.Close()
            bm

        [<Tests>]
        let tests = 
            testList "Bench Tests" [
                testList "Sample Tests" [
                    test "Expect sample duration to be 1 second" {
                        let s = createSampleWithMillionMessagesPerSecond 1
                        let duration = s.End - s.Start
                        let oneSec = TimeSpan(0, 0, 1)
                        Expect.equal s.Duration duration "Expected sample duration to be End - Start"
                        Expect.equal s.Duration oneSec "Expected sample duration to be 1 second" 
                    }

                    test "Expect sample seconds to be 1 second" {
                        let s = createSampleWithMillionMessagesPerSecond 1
                        let seconds = (s.End - s.Start).TotalSeconds
                        Expect.equal s.Seconds seconds "Expected sample seconds to be 1 second"
                        Expect.equal s.Seconds 1.0 "Expected sample seconds to be 1.0 second"
                    }
                    
                    test "Expect rate at 1 million messages" {
                        let s = createSampleWithMillionMessagesPerSecond 60
                        Expect.equal s.Rate (Million |> int64) "Expected sample rate at 1 million msgs"
                    }
                    
                    test $"Expect throughput at {MsgSize} million bytes/sec" {
                        let s = createSampleWithMillionMessagesPerSecond 60
                        Expect.equal s.Throughput (Million * MsgSize |> float) $"Expected throughput at {MsgSize} million bytes/sec"
                    }
                ]
                
                testList "SampleGroup Tests" [
                    test "Expect aggregate duration to be 2.0 seconds" {
                        let sg = SampleGroup.create ()
                        createSampleWithMillionMessagesPerSecond 1 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 2 |> SampleGroup.addSample sg
                        let agg = !sg.Aggregate
                        let twoSec = TimeSpan(0,0,2)
                        Expect.equal agg.Duration twoSec "Expected aggregate duration to be 2.0 seconds"
                    }
                    
                    test "Expect aggregate seconds to be 3.0 seconds" {
                        let sg = SampleGroup.create ()
                        createSampleWithMillionMessagesPerSecond 1 |> SampleGroup.addSample sg 
                        createSampleWithMillionMessagesPerSecond 2 |> SampleGroup.addSample sg 
                        createSampleWithMillionMessagesPerSecond 3 |> SampleGroup.addSample sg
                        let agg = !sg.Aggregate
                        Expect.equal agg.Seconds 3.0 "Expected aggregate seconds to be 3.0 seconds"
                    }
                    
                    test "Expect message rate at 2 million msgs/sec" {
                        let sg = SampleGroup.create ()
                        createSampleWithMillionMessagesPerSecond 1 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 2 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 3 |> SampleGroup.addSample sg
                        let agg = !sg.Aggregate
                        Expect.equal agg.Rate (Million * 2 |> int64) "Expected message rate at 2 million msgs/sec"
                    }
                    
                    test $"Expect throughput at {2 * MsgSize} million bytes/sec" {
                        let sg = SampleGroup.create ()
                        createSampleWithMillionMessagesPerSecond 1 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 2 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 3 |> SampleGroup.addSample sg
                        let agg = !sg.Aggregate
                        Expect.equal agg.Throughput (2 * Million * MsgSize |> float)
                                     $"Expected throughput of {2 * MsgSize} million bytes/sec"
                    }
                    
                    test "Expect equal MinRate & MaxRate" {
                        let sg = SampleGroup.create ()
                        createSampleWithMillionMessagesPerSecond 1 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 2 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 3 |> SampleGroup.addSample sg
                        Expect.equal (sg.MinRate ()) (sg.MaxRate ()) "Expected MinRate == MaxRate"  
                    }
                    
                    test "Expect equal MinRate & AvgRate" {
                        let sg = SampleGroup.create ()
                        createSampleWithMillionMessagesPerSecond 1 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 2 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 3 |> SampleGroup.addSample sg
                        Expect.equal (sg.MinRate ()) (sg.AverageRate ()) "Expected MinRate == MaxRate"  
                    }
                    
                    test "Expect standard deviation to be zero" {
                        let sg = SampleGroup.create ()
                        createSampleWithMillionMessagesPerSecond 1 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 2 |> SampleGroup.addSample sg
                        createSampleWithMillionMessagesPerSecond 3 |> SampleGroup.addSample sg
                        Expect.equal (sg.StdDev ()) (Some 0.0) "Expected standard deviation to be zero"
                    }
                ]
                
                testList "Benchmark Tests" [
                    test "Create a simple benchmark" {
                        let name = "test"
                        let bm = Benchmark.NewBenchmark(name, 1, 1)
                        bm.AddSubSample(createSampleWithMillionMessagesPerSecond 1)
                        bm.AddPubSample(createSampleWithMillionMessagesPerSecond 1)
                        bm.Close()
                        Expect.isNotNull bm.RunID "Expected RunID to be non null"
                        Expect.isNotEmpty bm.RunID "Expected benchmark to have a valid RunID"
                        Expect.isNotWhitespace bm.RunID "Expected RunID to have valid characters (not just whitespace)" 
                        Expect.equal bm.Pubs.Samples.Count 1 "Expected one publisher"
                        Expect.equal bm.Subs.Samples.Count 1 "Expected one subscriber"
                        let agg = !bm.Aggregate
                        Expect.equal agg.MessageCount (2 * Million |> int64) "Expected 2 million messages"
                        Expect.equal agg.IOBytes (2 * Million * MsgSize |> int64) $"Expected {2 * MsgSize} million bytes"
                        let oneSec = TimeSpan(0, 0, 1)
                        Expect.equal agg.Duration oneSec "Expected duration of 1 second"
                    }
                    
                    test "Expect CSV with 4 rows of 7 columns" {
                        let bm = makeBench 1 1
                        let csv = bm.CSV()
                        let rows = csv.Split('\n')
                        Expect.equal rows.Length 4 "Expected 4 rows in the CSV string"
                        
                        let fields = rows.[1].Split(',')
                        Expect.equal fields.Length 7 "Expected 7 fields in the CSV string"
                    }
                    
                    test "Expect short report with 4 lines of output" {
                        let bm = makeBench 0 1
                        let rpt = bm.Report()
                        let lines = rpt.Split('\n')
                        Expect.equal lines.Length 4 "Expected 4 lines of output: header, pub, sub, empty"
                    }
                    
                    test "Expect full report with 10 lines of output" {
                        let bm = makeBench 2 2
                        let rpt = bm.Report()
                        let lines = rpt.Split('\n')
                        Expect.equal lines.Length 10 "Expected 10 lines of output: header, pub header, pub x 2, stats, sub header, sub x 2, stats, empty"
                    }
                    
                    test "Expect 0 messages for 0 clients" {
                        let msgs = messagesPerClient 0 0
                        Expect.hasLength msgs 0 "Expected 0 length for 0 clients"
                    }
                    
                    test "Expect 1 messages for 2 clients" {
                        let msgs = messagesPerClient 1 2
                        Expect.hasLength msgs 2 "Expected length of 2 for 2 clients"
                        Expect.sequenceEqual msgs [| 1; 0 |] "Expected sequence of [| 1; 0 |]"
                    }
                    
                    test "Expect 2 messages for 2 clients" {
                        let msgs = messagesPerClient 2 2
                        Expect.hasLength msgs 2 "Expected length of 2 for 2 clients"
                        Expect.sequenceEqual msgs [| 1; 1 |] "Expected sequence of [| 1; 1 |]"
                    }
                    
                    test "Expect 3 messages for 3 clients" {
                        let msgs = messagesPerClient 3 2
                        Expect.hasLength msgs 2 "Expected length of 2 for 2 clients"
                        Expect.sequenceEqual msgs [| 2; 1 |] "Expected sequence of [| 1; 0 |]"
                    }
                ]
            ]

        [<EntryPoint>]
        let main argv =
            Tests.runTestsInAssembly defaultConfig argv
