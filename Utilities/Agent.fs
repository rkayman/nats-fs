namespace Aspir.Utilities

    open System.Threading

        
    module Agent =

        [<AbstractClass>]
        type Agent<'Msg>() =
            abstract member Agent : MailboxProcessor<'Msg>
            abstract member CancellationToken : CancellationToken 
            abstract member Tell        : 'Msg -> unit 
            abstract member Ask         : (AsyncReplyChannel<'Reply> -> 'Msg) * int option -> 'Reply
            abstract member TryAsk      : (AsyncReplyChannel<'Reply> -> 'Msg) * int option -> 'Reply option 
            abstract member AskAsync    : (AsyncReplyChannel<'Reply> -> 'Msg) * int option -> Async<'Reply>
            abstract member TryAskAsync : (AsyncReplyChannel<'Reply> -> 'Msg) * int option -> Async<'Reply option>
            abstract member Dispose     : unit -> unit 

            default this.Tell(message) = this.Agent.Post(message)
            default this.Ask(message, ?timeout) =
                timeout |> Option.map (fun t -> this.Agent.PostAndReply(message, t))
                        |> Option.defaultWith (fun _ -> this.Agent.PostAndReply(message))
            default this.TryAsk(message, ?timeout) =
                timeout |> Option.map (fun t -> this.Agent.TryPostAndReply(message, t))
                        |> Option.defaultWith (fun _ -> this.Agent.TryPostAndReply(message))
            default this.AskAsync(message, ?timeout) =
                timeout |> Option.map (fun t -> this.Agent.PostAndAsyncReply(message, t))
                        |> Option.defaultWith (fun _ -> this.Agent.PostAndAsyncReply(message))
            default this.TryAskAsync(message, ?timeout) =
                timeout |> Option.map (fun t -> this.Agent.PostAndTryAsyncReply(message, t))
                        |> Option.defaultWith (fun _ -> this.Agent.PostAndTryAsyncReply(message))

                
        module Remote =
            
            [<AbstractClass>]
            type RemoteAgent<'Msg>() =
                inherit Agent<'Msg>() 

                default this.Tell(message) = this.Agent.Post(message) 
