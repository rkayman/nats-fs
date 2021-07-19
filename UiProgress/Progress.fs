namespace UiProgress

module Console = 

    open System
    open System.Threading

    open Controls

    /// RefreshInterval in the default time duration to wait for refreshing the output 
    let DefaultRefreshInterval = TimeSpan.FromMilliseconds(33.0) 

    type ProgressState =
        { /// Pos is the top, leftmost position of the progress bar
          pos: struct (int * int) option 
          /// Width is the width of the progress bars 
          width: int
          /// Bars is the collection of progress bars 
          bars: Bar list 
          /// RefreshInterval in the time duration to wait for refreshing the output 
          refreshInterval: TimeSpan
          /// Ticker is the timer controlling the refresh rate of the progress bar 
          ticker: Timer option }
        
        static member CreateDefault() =
            { pos = None
              width = float(Console.WindowWidth) * 0.5 |> int
              bars = []
              refreshInterval = DefaultRefreshInterval
              ticker = None }

    type private ProgressCommand =
        | AddBar of int * AsyncReplyChannel<Bar> 
        | Start 
        | Stop 
        | SetRefreshInterval of TimeSpan 
        | Display
        | IsComplete of AsyncReplyChannel<bool> 
        | ShutDown of AsyncReplyChannel<unit> 
        
    type Progress(?state: ProgressState, ?cancel: CancellationToken) as this =
        let progress = state |> Option.defaultValue (ProgressState.CreateDefault())
        
        let addBar total state = 
            let bar = new Bar(total, state.width) 
            { state with bars = state.bars @ [bar] } 
            
        let newTimer () =
            Some (new Timer(this.TimerCallback, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan))
        
        let changeTimer (interval: TimeSpan) (t: Timer) = t.Change(interval, interval) |> ignore 

        let startTicker state =                
            let ticker' = state.ticker |> Option.orElseWith newTimer
            ticker' |> Option.iter (changeTimer state.refreshInterval)
            { state with ticker = ticker' }
                
        let stopTicker state =
            state.ticker |> Option.iter (changeTimer Timeout.InfiniteTimeSpan)
            state
            
        let write state =
            Console.CursorVisible <- false 
            let struct (curX, curY) = state.pos |> Option.defaultWith Console.GetCursorPosition
            Console.SetCursorPosition(curX, curY)
            for bar in state.bars do
                printfn $"{bar.ToString()}"
            Console.CursorVisible <- true 
            if state.pos.IsSome then state else { state with pos = Some (curX, curY) }
            
        let isComplete state =
            state.bars |> List.forall (fun x -> x.IsComplete) 
            
        let cts = new CancellationTokenSource()
        let token = cancel |> Option.defaultValue cts.Token 

        let agent =
            MailboxProcessor.Start((fun inbox ->
                let rec loop state = async {
                    if token.IsCancellationRequested then return () 
                    let! msg = inbox.Receive()
                    match msg with
                    | AddBar (total, ch) ->
                        let state' = state |> addBar total
                        let bar = state'.bars |> List.last
                        ch.Reply(bar) 
                        return! loop state' 
                    | Start ->
                        return! state |> startTicker |> loop 
                    | Stop ->
                        return! state |> stopTicker |> loop 
                    | SetRefreshInterval n -> 
                        return! loop { state with refreshInterval = n }
                    | Display ->
                        return! state |> write |> loop
                    | IsComplete ch ->
                        ch.Reply(isComplete state) 
                        return! loop state 
                    | ShutDown ch ->
                        state.ticker |> Option.iter (fun t -> t.Dispose()) 
                        state.bars |> List.iter (fun b -> b.Dispose())
                        ch.Reply() 
                }
                loop progress), token)
        
        member private _.TimerCallback _ = this.Display()

        member _.AddBar(total) = 
            let buildMessage channel = AddBar (total, channel) 
            agent.PostAndReply buildMessage
        
        member _.Display() = agent.Post Display
        
        member _.IsComplete with get () = agent.PostAndReply IsComplete
        
        member _.SetRefreshInterval(interval) = agent.Post (SetRefreshInterval interval)
        
        member _.Start() = agent.Post Start
        
        member _.Stop() = agent.Post Stop
        
        member x.Dispose() = (x :> IDisposable).Dispose() 
         
        interface IDisposable with
            member _.Dispose() =
                agent.PostAndReply ShutDown 
                cts.Dispose()
                (agent :> IDisposable).Dispose() 
