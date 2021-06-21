module UiProgress =

    open System
    open System.Globalization
    open System.Text
    open System.Threading

    type DateTime with 
        static member Since(start) = (-) DateTime.Now start 

    /// <summary> ErrMaxCurrentReached is the error when trying to set current value that exceeds the total value </summary>
    exception ErrMaxCurrentReached of string
    let errMaxCurrentReached = ErrMaxCurrentReached "errors: current value is greater total value"

    /// <summary> ElapsedStyle instructs how the elapsedTime is displayed </summary> 
    type ElapsedStyle = TotalSecondsStyle | DetailedStyle 

    /// <summary> The options used to display the progress bar </summary> 
    type BarState = {
            /// <summary> Total of the progress bar </summary> 
            total: int 
            /// <summary> LeftEnd is character in the left most part of the progress indicator. Defaults to '[' </summary> 
            leftEnd: char
            /// <summary> RightEnd is character in the right most part of the progress indicator. Defaults to ']' </summary> 
            rightEnd: char
            /// <summary> Fill is the character representing completed progress. Defaults to '=' </summary> 
            fill: char
            /// <summary> Head is the character that moves when progress is updated.  Defaults to '>' </summary> 
            head: char
            /// <summary> Empty is the character that represents the empty progress. Default is '-' </summary> 
            empty: char
            /// <summary> Width is the width of the progress bar. Default is 70</summary> 
            width: int
            /// <summary> TimeStarted is time progress began </summary>
            timeStarted: DateTime ValueOption 
            /// <summary> TimeElapsed is the time elapsed for the progress </summary>
            timeElapsed: TimeSpan ValueOption 
            /// <summary> </summary>
            elapsedStyle: ElapsedStyle 
            /// <summary> Current is the amount of progress made over elapsed time </summary> 
            current: int
            
            appendFuncs: DecoratorFunc list
            prependFuncs: DecoratorFunc list 
        }
        with static member CreateDefault() = {
                                                 total        = 100
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
                                                 prependFuncs = [] 
                                             }
    and
        /// <summary> DecoratorFunc is a function that can be prepended and appended to the progress bar </summary>
        DecoratorFunc = BarState -> string 

    type BuiltinFunc = PercentCompleted | TimeElapsed

    type internal Command =
        | SetValue of int 
        | IncrementBy of int * AsyncReplyChannel<bool>
        | GetCurrent of AsyncReplyChannel<int> 
        | AppendFunc of DecoratorFunc
        | PrependFunc of DecoratorFunc
        | Append of BuiltinFunc
        | Prepend of BuiltinFunc
        | ToString of AsyncReplyChannel<string> 

    type Bar(?barState: BarState) =
        let bar = barState |> Option.defaultValue (BarState.CreateDefault())
        
        let setValue n bar = if n > bar.total then raise errMaxCurrentReached else { bar with current = n }
        
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
            
        let cancel = new CancellationTokenSource() 
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
                | AppendFunc f -> return! bar |> appendFunc f |> loop
                | PrependFunc f -> return! bar |> prependFunc f |> loop
                | Append PercentCompleted -> return! bar |> appendFunc completedPercentString |> loop
                | Append TimeElapsed -> return! bar |> appendFunc timeElapsedString |> loop 
                | Prepend PercentCompleted -> return! bar |> prependFunc completedPercentString |> loop
                | Prepend TimeElapsed -> return! bar |> prependFunc timeElapsedString |> loop
                | ToString ch -> bar |> toString |> ch.Reply
                                 return! loop bar 
            }
            loop bar), cancel.Token)
        
        new(total: int, ?width: int) =
            let defaultState = BarState.CreateDefault() 
            let opts = match width with
                       | None -> { defaultState with total = total }
                       | Some w -> { defaultState with total = total; width = w }
            new Bar(opts)
            
        member _.Increment() =
            let buildMessage channel = IncrementBy(1, channel)
            agent.PostAndReply buildMessage
            
        member _.IncrementBy(n) =
            let buildMessage channel = IncrementBy(n, channel)
            agent.PostAndReply buildMessage
            
        member _.Value
            with get () = agent.PostAndReply GetCurrent
            and  set value = SetValue(value) |> agent.Post 

        member _.AppendFunc(f) = AppendFunc(f) |> agent.Post
        
        member _.PrependFunc(f) = PrependFunc(f) |> agent.Post
        
        member _.AppendCompleted() = Append PercentCompleted |> agent.Post
        
        member _.AppendElapsed() = Append TimeElapsed |> agent.Post
        
        member _.PrependCompleted() = Prepend PercentCompleted |> agent.Post
        
        member _.PrependElapsed() = Prepend TimeElapsed |> agent.Post 
            
        override _.ToString() = agent.PostAndReply ToString 
            
        interface IDisposable with
            member _.Dispose() =
                cancel.Dispose()


open System 
open UiProgress

let foo (total: int) (seconds: int) =
    async {
        let rnd = Random(total * seconds)
        use b = new Bar(total)
        b.AppendCompleted()
        b.PrependElapsed()
        
        while b.Value < total do
            let r = rnd.NextDouble()
            do! Async.Sleep(r * 1000.0 |> int)
            let inc = if total - b.Value < 2 then total - b.Value else int(float(total - b.Value) * r)
            b.IncrementBy(inc) |> ignore
            printfn $"{b.ToString()}"
            
    } |> Async.RunSynchronously
