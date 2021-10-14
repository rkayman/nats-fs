namespace Aspir.Utilities

    open System
    open System.Runtime.CompilerServices
    open System.Text
    open System.Threading


    module Console =
        
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


        module Controls = 
            type DateTime with 
                static member Since(start: DateTime) = DateTime.Now - start 

            /// ElapsedStyle articulates options for displaying elapsed time 
            type ElapsedStyle = TotalSecondsStyle | DetailedStyle 

            /// The options used to display the progress bar 
            type BarState =
                { /// Total of the progress bar
                  total: int
                  /// LeftEnd is character in the left most part of the progress indicator. Defaults to '['
                  leftEnd: char
                  /// RightEnd is character in the right most part of the progress indicator. Defaults to ']'
                  rightEnd: char
                  /// Fill is the character representing completed progress. Defaults to '=' 
                  fill: char
                  /// Head is the character that moves when progress is updated.  Defaults to '>' 
                  head: char
                  /// Empty is the character that represents the empty progress. Default is '-' 
                  empty: char
                  /// Width is the width of the progress bar. Default is 70 
                  width: int
                  /// TimeStarted is time progress began
                  timeStarted: DateTime ValueOption 
                  /// TimeElapsed is the time elapsed for the progress
                  timeElapsed: TimeSpan ValueOption 
                  /// ElapsedStyle controls how timeElapsed is displayed
                  elapsedStyle: ElapsedStyle 
                  /// Current is the amount of progress made over elapsed time 
                  current: int
                
                  appendFuncs: DecoratorFunc list
                  prependFuncs: DecoratorFunc list }

                static member CreateDefault() =
                    { total        = 100
                      leftEnd      = '['
                      rightEnd     = ']'
                      fill         = '='
                      head         = '>'
                      empty        = '-'
                      width        = 70
                      timeStarted  = ValueNone
                      timeElapsed  = ValueNone
                      elapsedStyle = TotalSecondsStyle 
                      current      = 0
                      appendFuncs  = [] 
                      prependFuncs = [] }

            and
                /// DecoratorFunc is a function that can be prepended and appended to the progress bar
                DecoratorFunc = BarState -> string 

            type BuiltinFunc = PercentCompleted | TimeElapsed

            type private BarCommand =
                | SetValue of int 
                | IncrementBy of int * AsyncReplyChannel<bool>
                | GetCurrent of AsyncReplyChannel<int>
                | IsComplete of AsyncReplyChannel<bool> 
                | AppendFunc of DecoratorFunc
                | PrependFunc of DecoratorFunc
                | Append of BuiltinFunc
                | Prepend of BuiltinFunc
                | ToString of AsyncReplyChannel<string>
                | Quit 

            /// <summary>
            /// ErrMaxCurrentReached is the error when trying to set current value that exceeds the total value
            /// </summary>
            exception ErrMaxCurrentReached of string

            type Bar(?barState: BarState, ?cancel: CancellationToken) as this =
                let bar = barState |> Option.defaultValue (BarState.CreateDefault())
                
                let setValue n bar = if n > bar.total
                                     then raise (ErrMaxCurrentReached "errors: current value is greater total value")
                                     else { bar with current = n }
                
                let incrementBy n bar =
                    match bar.current, bar.timeStarted with
                    | cur, _ when cur > Int32.MaxValue - n -> raise (invalidOp $"Cannot increment beyond {Int32.MaxValue}")
                    | cur, _ when cur + n > bar.total -> false, bar
                    | cur, ValueSome start ->
                        true, { bar with current = cur + n
                                         timeElapsed = DateTime.Since(start) |> ValueSome }
                    | cur, ValueNone ->
                        true, { bar with current = cur + n
                                         timeStarted = ValueSome DateTime.Now
                                         timeElapsed = ValueSome TimeSpan.Zero }
                        
                let isComplete bar = bar.current = bar.total 
                    
                let appendFunc f bar = { bar with BarState.appendFuncs = bar.appendFuncs @ [f] }
                
                let prependFunc f bar = { bar with BarState.prependFuncs = bar.prependFuncs @ [f] }
                
                let completedPercent bar = float(bar.current) / float(bar.total) * 100.00
                
                let completedPercentString bar = $"%3.0f{completedPercent bar}%%"
                    
                let prettyDuration style (dur: TimeSpan) =
                    let sb = StringBuilder(32)
                    let apply f g n = 
                        match f with
                        | false -> () 
                        | true -> Printf.bprintf sb g n 
                    let rec loop xs =
                        match xs with
                        | [] -> () 
                        | (num, filter, fmt)::xs' -> apply filter fmt num
                                                     loop xs'
                    let xs = match style with
                             | TotalSecondsStyle ->
                                 [(dur.TotalSeconds + 0.5 |> int, true, Printf.BuilderFormat<int->unit>("%5ds"))]
                             | DetailedStyle ->
                                 [(dur.Days, dur.Days > 0, Printf.BuilderFormat<int->unit>("%02dd:"))
                                  (dur.Hours, dur.Hours > 0, Printf.BuilderFormat<int->unit>("%02dh:"))
                                  (dur.Minutes, dur.Minutes > 0, Printf.BuilderFormat<int->unit>("%02dm:"))
                                  (dur.Seconds, dur.Seconds > 0, Printf.BuilderFormat<int->unit>("%02ds"))]
                    loop xs
                    sb.ToString() 
                    
                let timeElapsedString bar =
                    match bar.timeElapsed with
                    | ValueNone     -> "---"
                    | ValueSome dur -> prettyDuration bar.elapsedStyle dur
                    
                let toString bar =
                    let completedWidth = (0.5 + float(bar.width) * (completedPercent bar) / 100.0) |> int
                    let arr = Array.init bar.width (fun i -> if i < completedWidth - 1 then bar.fill
                                                             elif completedWidth > 0 && i = completedWidth - 1 then bar.head
                                                             else bar.empty)
                    arr.[0] <- bar.leftEnd
                    arr.[arr.Length - 1] <- bar.rightEnd
                    
                    let sb = StringBuilder(arr.Length * 2).Append(arr)
                    
                    let insert str = sb.Insert(0, $"{str} ") |> ignore 
                    bar.appendFuncs |> List.iter (fun f -> f bar |> Printf.bprintf sb " %s")
                    bar.prependFuncs |> List.iter (fun f -> f bar |> insert )
                    sb.ToString() 
                    
                let cts = new CancellationTokenSource()
                let token = cancel |> Option.defaultValue cts.Token 
                let agent = MailboxProcessor.Start((fun inbox ->
                    let rec loop bar = async {
                        let! msg = inbox.Receive()
                        match msg with
                        | SetValue x -> return! bar |> setValue x |> loop 
                        | IncrementBy (x, ch) ->
                            let success, bar' = bar |> incrementBy x
                            ch.Reply(success)
                            return! loop bar' 
                        | GetCurrent ch ->
                            ch.Reply(bar.current)
                            return! loop bar
                        | IsComplete ch ->
                            ch.Reply(isComplete bar) 
                        | AppendFunc f -> return! bar |> appendFunc f |> loop
                        | PrependFunc f -> return! bar |> prependFunc f |> loop
                        | Append PercentCompleted -> return! bar |> appendFunc completedPercentString |> loop
                        | Append TimeElapsed -> return! bar |> appendFunc timeElapsedString |> loop 
                        | Prepend PercentCompleted -> return! bar |> prependFunc completedPercentString |> loop
                        | Prepend TimeElapsed -> return! bar |> prependFunc timeElapsedString |> loop
                        | ToString ch -> bar |> toString |> ch.Reply
                                         return! loop bar
                        | Quit -> return ()
                    }
                    loop bar), token)
                
                new(total: int, ?width: int, ?cancel: CancellationToken) =
                    let defaultState = BarState.CreateDefault()
                    let bs = match width with
                             | None -> { defaultState with total = total }
                             | Some w -> { defaultState with total = total; width = w }
                    if cancel.IsNone then new Bar(bs) else new Bar(bs, cancel.Value)
                    
                member _.Increment() =
                    let buildMessage channel = IncrementBy(1, channel)
                    agent.PostAndReply buildMessage
                    
                member _.IncrementBy(n) =
                    let buildMessage channel = IncrementBy(n, channel)
                    agent.PostAndReply buildMessage
                    
                member _.IsComplete with get () = agent.PostAndReply IsComplete 
                    
                member _.Value
                    with get () = agent.PostAndReply GetCurrent
                    and  set value = SetValue(value) |> agent.Post 

                member _.AppendFunc(f) = AppendFunc(f) |> agent.Post
                
                member _.PrependFunc(f) = PrependFunc(f) |> agent.Post
                
                member _.AppendCompleted() = Append PercentCompleted |> agent.Post
                
                member _.AppendElapsed() = Append TimeElapsed |> agent.Post
                
                member _.PrependCompleted() = Prepend PercentCompleted |> agent.Post
                
                member _.PrependElapsed() = Prepend TimeElapsed |> agent.Post
                
                member _.Cancel() = cts.Cancel()
                
                member _.Dispose() = (this :> IDisposable).Dispose()
                    
                override _.ToString() = agent.PostAndReply ToString 
                    
                interface IDisposable with
                    member _.Dispose() =
                        agent.Post Quit 
                        (agent :> IDisposable).Dispose() 
                        cts.Dispose()


        open Controls
        
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
                  width = float(Console.WindowWidth) * 0.618 |> int
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

            let write state =
                Console.CursorVisible <- false
                let struct (curX, curY) = state.pos |> Option.defaultWith Console.GetCursorPosition
                let struct (x, y) = if state.pos.IsNone then (curX, curY) else (curX, curY - 2)
                Console.SetCursorPosition(x, y)
                state.bars |> List.iter (printfn "%A")
                Console.CursorVisible <- true 
                if state.pos.IsSome then state else { state with pos = Some (curX, curY) }
                
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
