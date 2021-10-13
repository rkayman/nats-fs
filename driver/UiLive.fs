namespace Aspir

open System
open System.Runtime.CompilerServices
open System.Text
open System.Threading

module Utilities = 

    module UiLive =

        /// RefreshInterval in the default time duration to wait for refreshing the output 
        let DefaultRefreshInterval = TimeSpan.FromMilliseconds(12.5) 

        type WriterState =
            { /// Height is the height of the terminal 
              height: int  
              /// Width is the width of the terminal 
              width: int
              /// Buffer is the buffer ready to be written to the terminal
              buffer: StringBuilder
              /// LineCount represents the number of lines (i.e. \n) previously written to the terminal
              lineCount: int 
              /// RefreshInterval in the time duration to wait for refreshing the output 
              refreshInterval: TimeSpan
              /// Ticker is the timer controlling the refresh rate of the progress bar 
              ticker: Timer option }
            
            static member CreateDefault() =
                let struct (h, w) = struct (Console.WindowHeight, Console.WindowWidth)
                { height          = h
                  width           = w
                  buffer          = StringBuilder(h * w * 4)
                  lineCount       = 0 
                  refreshInterval = DefaultRefreshInterval
                  ticker          = None }

        type private ConsoleCommand =
            | Write of string
            | Start 
            | Stop 
            | Refresh of TimeSpan 
            | Flush 
            | ShutDown of AsyncReplyChannel<unit> 
            
        type Writer(?state: WriterState, ?cancel: CancellationToken) as this =

            let writer = state |> Option.defaultValue (WriterState.CreateDefault())
                
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

#if !Windows 
            [<Literal>]
            let ESC = '\027'
            let clear = $"%c{ESC}[1A%c{ESC}[2K"
#endif

            [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
            let clearLines cnt =
#if !Windows
                clear |> String.replicate cnt |> printf "%s"
#else
                Console.SetCursorPosition(0, Console.CursorTop - cnt)
                let clr = " " |> String.replicate Console.WindowWidth
                for i = 1 to cnt do 
                    printf $"{clr}"
                Console.SetCursorPosition(0, Console.CursorTop - cnt)
#endif
            
            [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
            let write value state =
                state.buffer.AppendLine(value) |> ignore 
                state
                
            [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
            let countLines (ch : char) (width : int) (lines : int byref) (curLineChars : int byref) =
                if ch = '\n' || width < curLineChars + 1 then
                    lines <- lines + 1
                    curLineChars <- 0
                else
                    curLineChars <- curLineChars + 1
                
            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]
            let flush state =
                let buf = state.buffer 
                if buf.Length = 0 then
                    state
                else
                    clearLines state.lineCount
                    
                    let mutable lines = 0
                    let mutable curLineChars = 0 
                    for chunk in buf.GetChunks() do
                        let span = chunk.Span
                        for i = 0 to span.Length do
                            countLines span.[i] state.width &lines &curLineChars
                                
                    printf $"{buf}\n"
                    buf.Clear() |> ignore 
                    { state with lineCount = lines }
                    
            let cts = new CancellationTokenSource()
            let token = cancel |> Option.defaultValue cts.Token 

            let agent =
                MailboxProcessor.Start((fun inbox ->
                    let rec loop state = async {
                        if token.IsCancellationRequested then return () 
                        let! msg = inbox.Receive()
                        match msg with
                        | Write value ->
                            return! state |> write value |> loop 
                        | Flush ->
                            return! state |> flush |> loop
                        | Start ->
                            return! state |> startTicker |> loop 
                        | Stop ->
                            return! state |> stopTicker |> loop 
                        | Refresh interval -> 
                            return! loop { state with refreshInterval = interval }
                        | ShutDown ch ->
                            state.ticker |> Option.iter (fun t -> t.Dispose()) 
                            ch.Reply() 
                    }
                    loop writer), token)
            
            member private _.TimerCallback _ = this.Flush()
            
            member _.Write(value) = agent.Post (Write value)
            
            member _.Flush() = agent.Post Flush 
            
            member _.SetRefreshInterval(interval) = agent.Post (Refresh interval)
            
            member _.Start() = agent.Post Start
            
            member _.Stop() = agent.Post Stop
            
            member x.Dispose() = (x :> IDisposable).Dispose() 
             
            interface IDisposable with
                member _.Dispose() =
                    agent.PostAndReply ShutDown 
                    cts.Dispose()
                    (agent :> IDisposable).Dispose() 
