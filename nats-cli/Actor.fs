namespace Aspir.Messaging

open System
open System.Threading
open System.Threading.Tasks
open System.Threading.Channels
open FSharp.Control.Tasks.NonAffine

    
module Actor = 

    // A high-performance actor; potential alternative to MailboxProcessor,
    // built-in F# implementation. This actor implementation uses
    // System.Threading.Channel.Channel and Ply to achieve its performance.
    // TODO testing
    // TODO more features
    // TODO lost letters
    // TODO unhandled exceptions
    // TODO exception handling exceptions

    /// One option for handling exceptions is to extend actor message types
    /// with a exception handler; for message cases that have a
    /// TaskCompletionSource, the implementation can use TrySetException on
    /// that; this allows exceptions to be caught without necessarily having
    /// to use try-catch blocks for each message case.
    [<RequireQualifiedAccess>]
    type IActorMessage =
        // If the handler throws an exception, then this can be propagated back to the caller if
        // it has a callback mechanism; otherwise it is lost.
        // TODO "lost letters" outbox for unhandled top-level exceptions.
        abstract member TryHandleException : Exception -> bool

    /// The states that an actor can be in.
    [<RequireQualifiedAccess>]
    [<Struct>]
    type ActorLifetimeState =
        /// New state; actor not yet started.
        | New
        /// Actor is now running and accepting messages.
        | Started
        /// Actor is stopping; it will not accept new messages
        /// but it will process all pending messages queued up.
        | Stopping
        /// Actor has stopped and is in a terminal state.
        | Stopped

    /// Actor.
    type IActor<'Msg> =
        inherit IDisposable
        // Queries
        /// Lifecycle state.
        abstract member LifetimeState : ActorLifetimeState
        // Commands (Lifecycle)
        // Start the actor.
        abstract member Start : unit -> unit
        // Process all queued up messages and then stop the actor.
        abstract member TryStop : unit -> bool
        // Stop the actor immediately.
        abstract member Stop : unit -> unit
        // Commands (Communication)
        // Fire and forget.
        abstract member Tell : 'Msg -> unit
        // Ask and reply synchronously.
        abstract member Ask : (TaskCompletionSource<'Reply> -> 'Msg) -> 'Reply
        // Ask and reply asynchronously.
        abstract member AskAsync : (TaskCompletionSource<'Reply> -> 'Msg) -> ValueTask<'Reply>

    /// Actor message handler.
    and ActorMessageHandler<'Msg,'State> =
        Actor<'Msg,'State> -> 'State -> 'Msg -> ValueTask<'State>
        
    /// Actor error handler
    and ActorErrorHandler<'Msg,'State> =
        Actor<'Msg,'State> -> 'State -> 'Msg -> Exception -> bool

    /// Actor implementation.
    and Actor<'Msg,'State>
        private
            (inbox : Channel<'Msg>,
             messageHandler : ActorMessageHandler<'Msg,'State>,
             actorState : 'State, 
             errorHandler : ActorErrorHandler<'Msg,'State> option) =

        let gate = obj()
        let cts = new CancellationTokenSource()
        let mutable lifetimeState = ActorLifetimeState.New
        let mutable worker = None
        
        let assertStarted () =
            match lifetimeState with
            | ActorLifetimeState.Started -> ()
            | ActorLifetimeState.New -> failwith "Actor not started."
            | ActorLifetimeState.Stopped -> failwith "Actor stopped."
            | ActorLifetimeState.Stopping -> failwith "Actor stopping."

        let work (self: Actor<'Msg,'State>) (cancellationToken: CancellationToken) =
            vtask {
                lifetimeState <- ActorLifetimeState.Started
                try
                    let mutable state = actorState 
                    while not cancellationToken.IsCancellationRequested do
                        let! ok = inbox.Reader.WaitToReadAsync(cancellationToken)
                        let mutable read = ok
                        while read do
                            let ok, msg = inbox.Reader.TryRead()
                            read <- ok
                            if ok then
                                try
                                    let! st = messageHandler self state msg
                                    state <- st 
                                with messageHandlerEx ->
                                    let handleErr = match box msg with
                                                    | :? IActorMessage as actorMsg -> actorMsg.TryHandleException |> Some
                                                    | _  -> errorHandler |> Option.map (fun f -> f self state msg) 
                                    try
                                        match handleErr with
                                        | None -> failwith "Missing error handler"
                                        | Some h -> 
                                            if not (h messageHandlerEx) then
                                                ()  // TODO internal error propagation
                                    with exceptionHandlerEx ->
                                        ()  // TODO internal error propagation

                finally
                    lifetimeState <- ActorLifetimeState.Stopped
            }
            
        static member Create (initState : 'State, 
                              handler : ActorMessageHandler<'Msg,'State>,
                              ?errorHandler: ActorErrorHandler<'Msg,'State>) : IActor<'Msg> =
            let opt = UnboundedChannelOptions()
            opt.SingleReader <- true
            opt.SingleWriter <- true 
            let ch = Channel.CreateUnbounded<'Msg>(opt)
            if typedefof<'Msg>.IsAssignableTo(typeof<IActorMessage>) then 
                new Actor<'Msg,'State>(ch, handler, initState, None) :> IActor<'Msg>
            elif errorHandler.IsSome then
                new Actor<'Msg,'State>(ch, handler, initState, errorHandler) :> IActor<'Msg>
            else
                invalidArg (nameof errorHandler) "When message type is not derived from IActorMessage, then errorHandler is required."

        interface IActor<'Msg> with
        
            member this.LifetimeState : ActorLifetimeState =
                lifetimeState
            
            member this.Start() =
                if worker.IsNone then
                    lock gate (fun () ->
                        if worker.IsNone then
                            worker <- Some (work this cts.Token)
                    )
        
            member this.TryStop() =
                try
                    (this :> IActor<'Msg>).Stop()
                    true
                with ex ->
                    eprintfn $"[Actor Error] %A{ex}"
                    false
        
            member this.Stop() =
                lock gate (fun () ->
                    match lifetimeState with
                    | ActorLifetimeState.New ->
                        failwith "Actor must have started first."
                    | ActorLifetimeState.Started
                    | ActorLifetimeState.Stopping ->
                        if not (inbox.Writer.TryComplete()) then
                            failwith "Error stopping actor (unable to complete and close the inbox channel)."
                        cts.Cancel()
                    | ActorLifetimeState.Stopped ->
                        () // Idempotent
                )
        
            member this.Tell(message : 'Msg) : unit =
                assertStarted()
                if not (inbox.Writer.TryWrite(message)) then
                    failwith "Unexpected write failure (unbounded channel write failure)."
        
            member this.Ask(messageBuilder : TaskCompletionSource<'Reply> -> 'Msg) : 'Reply =
                (this :> IActor<_>).AskAsync(messageBuilder).GetAwaiter().GetResult()
        
            member this.AskAsync(messageBuilder : TaskCompletionSource<'Reply> -> 'Msg) : ValueTask<'Reply> =
                assertStarted()
                let completion = new TaskCompletionSource<'Reply>()
                let message = messageBuilder completion
                (this :> IActor<_>).Tell(message)
                ValueTask<'Reply>(completion.Task)

            member this.Dispose() =
                lock gate (fun () -> if lifetimeState <> ActorLifetimeState.New then (this :> IActor<'Msg>).Stop())
