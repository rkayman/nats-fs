namespace Aspir

open System
open System.Text
open System.Threading
open FSharp.Control.Tasks.NonAffine

open Aspir.Utilities.Console
open Messaging.Nats.Client
open Messaging.Nats.Client.Actor

module Driver =
        
    type DumpState = { mutable cnt: int; out: Writer }
    
    [<EntryPoint>]
    let main argv =
        let expectedArgs = 2
        if Array.length argv <> expectedArgs then
            failwith $"{expectedArgs} arguments required: <number of subscribers> <subject on which to listen>"

        let numSub = argv.[0] |> Int32.Parse
        let subject = argv.[1] 

        let conn = connect {
            name "F# Wrapper for .NET NATS Client"
            url "localhost"
            onClosed (fun _ args -> printfn $"Connection '{args.Conn.Opts.Name}' closed")
            onDisconnected (fun _ args -> printfn $"Connection '{args.Conn.Opts.Name}' disconnected")
            onReconnected (fun _ args -> printfn $"Connection '{args.Conn.Opts.Name}' reconnected")
        }
        conn.Start()

        let cts = new CancellationTokenSource()
        let writer = new Writer(cancel = cts.Token)
        writer.Start()
        
        let body _ state message =
            vtask {
                match message with
                | Observed msg ->
                    let st = Option.get state.actorState
                    let payload = Encoding.UTF8.GetString(msg.Data)
                    //st.out.Write $"[#{Interlocked.Increment(&st.cnt)}] Received on \"%s{msg.Subject}\"\n%s{payload}\n"
                    printfn $"[#{Interlocked.Increment(&st.cnt)}] Received on \"%s{msg.Subject}\"\n%s{payload}\n"  
                    return state
                | _ -> return state 
            }
            
        let error _ state message err =
            let st = Option.get state.actorState
            st.out.Write $"[ERROR] (msg='%A{message}')\n[DETAILS] %A{err}\n"
            true 
    
        use sub = subscribe {
            withConnection conn
            topic subject
            messageHandler body
            errorHandler error
            actorState (Some { cnt = 0; out = writer }) 
        }
        sub.Start()

        Console.ReadKey() |> ignore 

        sub.Stop()
        conn.Stop() 
        writer.Flush()
        writer.Stop()
        cts.Cancel()

        0
