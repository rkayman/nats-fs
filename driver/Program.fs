namespace Aspir

    open System
    open System.Runtime.CompilerServices
    open System.Text
    open System.Threading

    open Utilities.Console
    open Messaging.Nats.Client
    open Messaging.Nats.Client.Actor
    

    module Driver =

        [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
        let body (writer: WriterAgent) (inbox: MailboxProcessor<_>) =
            let rec loop cnt = async {
                let! (Observed msg) = inbox.Receive()
                let payload = Encoding.UTF8.GetString(msg.Data)
                let cnt' = cnt + 1
                writer.Write $"[#{cnt'}] Received on \"%s{msg.Subject}\"\n%s{payload}\n\n"
//                printf $"[#{cnt'}] Received on \"%s{msg.Subject}\"\n%s{payload}\n\n"
                return! loop cnt' 
            }
            loop 0 
            
        [<EntryPoint>]
        let main argv =
            let expectedArgs = 1
            if Array.length argv <> expectedArgs then
                failwith $"{expectedArgs} arguments required: <number of subscribers> <subject on which to listen>"

            use cts = new CancellationTokenSource()
            use writer = new WriterAgent(cancellationToken = cts.Token)
            try
                try 
                    let subject = argv.[0] 
                    //let numSub = argv.[0] |> Int32.Parse
                    
                    use conn = connect {
                        name "F# Wrapper for .NET NATS Client"
                        url "localhost"
                        onClosed (fun _ args -> printfn $"Connection '{args.Conn.Opts.Name}' closed")
                        onDisconnected (fun _ args -> printfn $"Connection '{args.Conn.Opts.Name}' disconnected")
                        onReconnected (fun _ args -> printfn $"Connection '{args.Conn.Opts.Name}' reconnected")
                    }

                    writer.Start() 
                    let agentBody = body writer 
                    use sub = subscribe {
                        withConnection conn
                        topic subject
                        body agentBody
                    }

                    Console.ReadKey() |> ignore
                    
                with ex ->
                    eprintfn $"[ERROR] %A{ex}" 

            finally 
                writer.Flush()
                writer.Stop()
                cts.Cancel()

            0
