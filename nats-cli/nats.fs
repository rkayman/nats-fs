namespace Aspir.Messaging.Nats 

    open System
    open System.Collections.Generic
    open System.Reactive.Concurrency
    open System.Reactive.Linq
    open System.Threading
    open FsToolkit.ErrorHandling
    open NATS.Client
    open NATS.Client.Rx

    
    [<AutoOpen>]
    module Client =
        
        module Actor =
            
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
                            

            exception SubscriptionKeyAlreadyExists of string 
            
            type NatsMessage =
                | Observed of Msg

            type ConnectionMessage =
                | ConnectToServer of config: Options
                | SubscribeToTopic of topic: string
                                      * body: (MailboxProcessor<NatsMessage> -> Async<unit>) 
                                      * reply: AsyncReplyChannel<Result<SubscriptionAgent, Exception>>
            
            and SubscriptionAgent internal (connection: IConnection,
                                            topic: string,
                                            body: MailboxProcessor<NatsMessage> -> Async<unit>,
                                            ?cancellationToken: CancellationToken,
                                            ?observerThread: IScheduler) =
                inherit Agent<NatsMessage>()

                let cts = new CancellationTokenSource()

                let token = cancellationToken |> Option.defaultValue cts.Token
                
                let agent = MailboxProcessor.Start(body, token)
                
                let thread = observerThread |> Option.defaultValue (ThreadPoolScheduler.Instance :> IScheduler)
                
                let sub = connection.Observe(topic)
                                    .ObserveOn(thread)
                                    .Select(Observed)
                                    .Subscribe(agent.Post) 
                
                override __.Agent with get() = agent 
                
                override __.CancellationToken with get() = token 
                
                override this.Dispose() = (this :> IDisposable).Dispose() 
                
                interface IDisposable with
                    member __.Dispose() =
                        sub.Dispose() 
                        cts.Dispose()
                        (agent :> IDisposable).Dispose() 
                
            and ConnectionAgent(?cancellationToken: CancellationToken) =
                inherit Agent<ConnectionMessage>()

                let cts = new CancellationTokenSource()
                
                let token = cancellationToken |> Option.defaultValue cts.Token
                
                let subscriptions = Dictionary<string, SubscriptionAgent>(5)
                
                let factory = ConnectionFactory()
                
                let thread = NewThreadScheduler.Default 
                
                let agent =
                    MailboxProcessor.Start((fun inbox ->
                        let rec running (conn: IConnection)
                                     (subs: IDictionary<string, SubscriptionAgent>) =
                            async {
                                let! msg = inbox.Receive()
                                match msg with
                                | SubscribeToTopic (topic, body, reply) ->
                                    match subs.TryGetValue(topic) with
                                    | false, _ ->
                                        let sub = new SubscriptionAgent(conn, topic, body, token, thread)
                                        subs.Add(topic, sub)
                                        reply.Reply(Ok sub) 

                                    | true, _ ->
                                        let ex = SubscriptionKeyAlreadyExists $"A subscription already exists for '{topic}'"
                                        reply.Reply(Error ex) 

                                    return! running conn subs

                                | _ ->
                                    eprintfn $"[WARNING] Connection '{conn.Opts.Name}' is already open."
                                    return! running conn subs 
                            }
                        and starting subs =
                            async {
                                let! msg = inbox.Receive()
                                match msg with
                                | ConnectToServer cfg ->
                                    let conn = factory.CreateConnection(cfg)
                                    return! running conn subs
                                | _ ->
                                    eprintfn $"[ERROR] A connection is required to subscribe. Make sure there is an open connection."
                                    return! starting subs 
                            }
                        starting subscriptions), token)
                
                override this.Agent with get() = agent
                
                override this.CancellationToken with get() = token
                
                override this.Dispose() = (this :> IDisposable).Dispose()
                
                interface IDisposable with
                    member __.Dispose() =
                        cts.Dispose()
                        (agent :> IDisposable).Dispose() 
                
        
        open Actor
        
        type SecretProviders =
            | AzureKeyVault of foo: unit 
            | Custom of bar: unit 
            
        type UserCredentials =
            | UseNKey of nKey: string * credentials: NKeyCredentials
            | UsePaths of credentialPath: string * privateKeyPath: string option
            | UseJwt of jwtEvent: EventHandler<UserJWTEventArgs> *
                        signatureEvent: EventHandler<UserSignatureEventArgs>
                        
        and NKeyCredentials =
            | PrivateKey of path: string
            | Callback of handler: EventHandler<UserSignatureEventArgs>

        type ConnectionBuilderState = { config: Options }
        
        /// ```fsharp
        ///
        /// let conn = connect {
        ///     useSecrets (AzureKeyVault (...))
        ///     name "Name of this Client"
        ///     customInboxPrefix "_foobar."
        ///     verbose false
        ///     noEcho false
        ///     subscriberDeliveryTaskCount 5 
        ///     secure true
        ///     user "nats_user"
        ///     password "secret_password"
        ///     token "magic_token"
        ///     url "nats://[username:password@]127.0.0.1:4222"     // option 1
        ///     url "tls://[username:password@]127.0.0.1:4222"      // option 2
        ///     url "nats://[token@]127.0.0.1:4222"                 // option 3
        ///     servers [| "nats://[username:password@]127.0.0.1:4222"
        ///                "tls://[username:password@]127.0.0.1:4222"
        ///                "nats://[token@]127.0.0.1:4222" |]
        ///     onClosed handler    // (fun connEventArgs -> ...)
        ///     onDisconnected handler
        ///     onReconnected handler
        ///     onServerDiscovered handler
        ///     onLameDuckMode handler 
        ///     asyncErrorOccurred handler  // (fun errEventArgs -> ...)
        ///     remoteCertValidator callback
        ///     userCredentials (UseNKey (nKey, PrivateKey pvtKeyPath)) 
        /// } 
        type ConnectionBuilder internal () =
            // TODO: Make tuple that includes a dictionary of KVs from the secret store
            member _.Yield _ =
                { config = ConnectionFactory.GetDefaultOptions() } 

            [<CustomOperation("secretProvider")>]
            member _.SecretProvider(state, value) =
                // TODO: setup secret provider
                NotImplementedException("SecretProvider not yet implemented") |> raise 
                match value with
                | AzureKeyVault _ -> state
                | Custom _ -> state 
            
            [<CustomOperation("name")>]
            member _.Name(state, value) =
                // TODO: apply validation logic to value
                state.config.Name <- value
                state 
                
            [<CustomOperation("customInboxPrefix")>]
            member _.CustomInboxPrefix(state, value) =
                // TODO: apply validation logic to value
                state.config.CustomInboxPrefix <- value
                state
                    
            [<CustomOperation("verbose")>]
            member _.Verbose(state, value) =
                state.config.Verbose <- value
                state 
                
            [<CustomOperation("noEcho")>]
            member _.NoEcho(state, value) =
                state.config.NoEcho <- value
                state 
                
            [<CustomOperation("subscriberDeliveryTaskCount")>]
            member _.SubscriberDeliveryTaskCount(state, value) =
                state.config.SubscriberDeliveryTaskCount <- value
                state 
                
            [<CustomOperation("secure")>]
            member _.Secure(state, value) =
                state.config.Secure <- value
                state
                
            [<CustomOperation("user")>]
            member _.User(state, value) =
                state.config.User <- value
                state 
                
            [<CustomOperation("password")>]
            member _.Password(state, value) =
                state.config.Password <- value
                state 
                
            [<CustomOperation("token")>]
            member _.Token(state, value) =
                state.config.Token <- value
                state 
                
            [<CustomOperation("url")>]
            member _.Url(state, value) =
                // TODO: apply validation logic to value 
                state.config.Url <- value
                state 
                
            [<CustomOperation("servers")>]
            member _.Servers(state, value) =
                // TODO: apply validation logic to each item in value
                state.config.Servers <- value
                state 
                
            [<CustomOperation("userCredentials")>]
            member _.UserCredentials(state, value) =
                match value with
                | UseNKey (key, PrivateKey path)       -> state.config.SetNkey(key, path)
                | UseNKey (key, Callback handler)      -> state.config.SetNkey(key, handler)
                | UsePaths (credPath, None)            -> state.config.SetUserCredentials(credPath) 
                | UsePaths (credPath, Some pvtKeyPath) -> state.config.SetUserCredentials(credPath, pvtKeyPath) 
                | UseJwt (jwtEvent, sigEvent)          -> state.config.SetUserCredentialHandlers(jwtEvent, sigEvent) 
                state
            
            [<CustomOperation("onDisconnected")>]
            member _.OnDisconnected(state, value) =
                let cfg = state.config
                let handler = EventHandler<ConnEventArgs>(value)
                cfg.DisconnectedEventHandler <- handler
                state 
                
            [<CustomOperation("onClosed")>]
            member _.OnClosed(state, value) =
                let cfg = state.config
                let handler = EventHandler<ConnEventArgs>(value)
                cfg.ClosedEventHandler <- handler
                state 

            [<CustomOperation("onReconnected")>]
            member _.OnReconnected(state, value) =
                let cfg = state.config
                let handler = EventHandler<ConnEventArgs>(value)
                cfg.ReconnectedEventHandler <- handler
                state 
                
            [<CustomOperation("onServerDiscovered")>]
            member _.OnServerDiscovered(state, value) =
                let cfg = state.config
                let handler = EventHandler<ConnEventArgs>(value)
                cfg.ServerDiscoveredEventHandler <- handler
                state
                
            [<CustomOperation("onLameDuckMode")>]
            member _.OnLameDuckMode(state, value) =
                let cfg = state.config
                let handler = EventHandler<ConnEventArgs>(value)
                cfg.LameDuckModeEventHandler <- handler
                state 

            [<CustomOperation("asyncErrorOccurred")>]
            member _.AsyncErrorOccurred(state, errHandler) =
                state.config.AsyncErrorEventHandler <- errHandler
                state
                
            [<CustomOperation("remoteCertValidator")>]
            member _.RemoteCertValidator(state, callback) =
                state.config.TLSRemoteCertificationValidationCallback <- callback
                state 

            member _.Run(state) =
                let { config = cfg } = state
                let agent = new ConnectionAgent()
                agent.Tell (ConnectToServer cfg)
                agent 
                
        let connect = ConnectionBuilder() 

        
        type SubscriptionBuilderState<'a> =
            { connAgent : ConnectionAgent option
              topic : string option
              body : (MailboxProcessor<NatsMessage> -> Async<unit>) option }

        let private validateConnectionActor (actor: ConnectionAgent option) = // (actor: IActor<Actor.ConnectionMessage<_>> option) =
            match actor with
            | None -> Error "Missing required NATS connection actor"
            | Some t -> Ok t
            
        let private validateTopic (topic: string option) =
            match topic with
            | None -> Error "A Topic (Subject) is required to make a subscription"
            | Some t ->
                if String.IsNullOrWhiteSpace(t) then
                    Error "Topic (Subject) cannot be null or empty"
                else
                    Ok t

        let private validateBody (body: (MailboxProcessor<NatsMessage> -> Async<unit>) option) =
            match body with
            | None -> Error "The subscription actor requires a message handler to operate"
            | Some t -> Ok t
            
        let private validateSubscriptionBuilderState state =
            validation {
                let! conn = validateConnectionActor state.connAgent
                and! topic = validateTopic state.topic
                and! body = validateBody state.body 
                return {| connAgent = conn; topic = topic; body = body |}
            }
        
        /// ```fsharp
        /// 
        /// let sub = subscribe {
        ///     useConnection conn
        ///     topic "aspir.workflows.>"
        ///     body (MailboxProcessor<NatsMessage -> Async<unit>)
        ///
        ///     (** FUTURE **)
        ///     actorForTopic initialState messageHandler exceptionHandler
        ///     actorScale min max 
        ///     actorPerTopic [| specific; topics; here |]
        ///     startAfter duration
        ///     startAt timeOfDay
        ///     endAfter (Items 10) | (Duration duration)
        ///     endAt timeOfDay
        ///
        ///     (** Add capabilities to create based on NATS queues or subject wildcards **)
        ///     TODO: nats pub traveler.<empId>.<travels>.<juris>
        ///     TODO: nats pub --queue=Travels --reply=__INBOX.ab456fd traveler.<empId>.<rewards>.<juris>
        ///     TODO: sub traveler.>
        ///     TODO: sub traveler.*.<travels>.* 
        /// } 
        type SubscriptionBuilder internal () =
            
            member _.Yield _ =
                { conn = None
                  topic = None
                  body = None }
                
            [<CustomOperation("withConnection")>]
            member _.WithConnection(state, value) =
                { state with connAgent = Some value }
                
            [<CustomOperation("topic")>]
            member _.Topic(state, value) =
                { state with topic = Some value }
                
<<<<<<< Updated upstream
            [<CustomOperation("body")>]
            member _.Body(state, value) =
                { state with body = Some value }
                                
            member _.Run(state) =
                match validateSubscriptionBuilderState state with
                | Error xs ->
                    failwith $"[Error] Unable to create subscription actor owing to the following errors:\n\t{xs}"
                | Ok x ->
                      let message = fun ch -> SubscribeToTopic (x.topic, x.body, ch)
                      x.connAgent.Ask(message, None)
                      |> Result.valueOr (fun ex -> failwith $"[ERROR] {ex.ToString()}")
                
        let subscribe = SubscriptionBuilder() 

        
        type AgentDelegate = MailboxProcessor<NatsMessage> -> Async<unit> 
        /// ```fsharp
        /// 
        /// let svc = service {
        ///     useConnection conn
        ///     listenTo "aspir.workflows.>" AgentPerTopic AgentDelegate 
        ///     publishTo "aspir.travelers.<empId>.<pnr>.<juris>" AgentDelegate 
        ///     ** needed? ** replyTo "aspir.travelers.<empId>.travels" AgentDelegate 
        ///
        ///     (** FUTURE **)
        ///     type AgentInstancing = AgentPerTopic | AgentSingleton
        ///     type ScalePolicy = Scale min: int * max: int 
        ///     type StartPolicy =
        ///         | Immediately
        ///         | StartAfter duration: TimeSpan
        ///         | StartAt timeOfDay: DateTimeOffset
        ///     type EndPolicy =
        ///         | EndAt timeOfDate: DateTimeOffset
        ///         | EndAfter duration: TimeSpan
        ///         | Take messageCount: int
        ///         | Naturally 
        ///
        ///     (** FUTURE **)
        ///     listenTo <topic> <instancing> ([Take n] | [Time t] | [TakeOrTime n t]) <agentDelegate>
        ///     publishTo <topic> ([SendAfter duration] | [SendAt timeOfDay]) (Repeat <see below>) <agentDelegate> 
        ///     repeat [Every 2<sec|min|hrs|days|wks|mos> | Monthly (Day 1) | ...]
        ///     retry RetryPolicy 
        ///
        ///     (** Add capabilities to create based on NATS queues or subject wildcards **)
        ///     TODO: nats pub traveler.<empId>.<travels>.<juris>
        ///     TODO: nats pub --reply=__INBOX.ab456fd traveler.<empId>.<rewards>.<juris>
        ///     TODO: sub traveler.>
        ///     TODO: sub --queue=Travels traveler.*.<travels>.* 
        /// } 
        type ServiceBuilder internal () =
            
            member _.Yield _ =
                { connAgent = None
                  topic = None
                  body = None }
                
            [<CustomOperation("withConnection")>]
            member _.WithConnection(state, value) =
                { state with connAgent = Some value }
                
            [<CustomOperation("topic")>]
            member _.Topic(state, value) =
                { state with topic = Some value }
                
            [<CustomOperation("body")>]
            member _.Body(state, value) =
                { state with body = Some value }
                                
            member _.Run(state) =
                match validateSubscriptionBuilderState state with
                | Error xs ->
                    failwith $"[Error] Unable to create subscription actor owing to the following errors:\n\t{xs}"
                | Ok x ->
                      let message = fun ch -> SubscribeToTopic (x.topic, x.body, ch)
                      x.connAgent.Ask(message, None)
                      |> Result.valueOr (fun ex -> failwith $"[ERROR] {ex.ToString()}")
                
        let service = ServiceBuilder() 

        
//        type PublisherBuilderState<'a> =
//            { conn : IActor<Actor.ConnectionMessage<'a>> option }
            
        /// ```fsharp
        ///
        /// let pub subj hdr msg = publish {
        ///     useConnection conn
        ///     topic $"aspir.workflows.%s{subj}"
        ///     payload hdr msg 
        ///
        ///     (** FUTURE **)
        ///     sendAfter duration
        ///     sendAt timeOfDay
        ///     repeat [Every 2<sec|min|hrs|days|wks|mos> | Monthly (Day 1) | ...]
        ///     retry RetryPolicy 
        /// }
//        type PublisherBuilder internal () =
//            member _.Yield _ =
//                { conn = None }
//                
//            [<CustomOperation("useConnection")>]
//            member _.UseConnection(state, value) : PublisherBuilderState<_> =
//                { state with conn = Some value }
                
                
//        let publish = PublisherBuilder()
        
        // TODO: Change example to match request CE
        /// ```fsharp
        ///
        /// let req subj hdr msg = request {
        ///     useConnection conn
        ///     subject $"aspir.workflows.%s{subj}"
        ///     payload hdr msg
        ///     listen (First replyHandler excHandler [initState])
        ///     listen (Time 1<sec> replyHandler excHandler [initState])
        ///     listen (Count 3 replyHandler excHandler [initState])
        ///     listen (CountTime 3 1<sec> replyHandler excHandler [initState])
        ///
        ///     (** FUTURE **)
        ///     sendAfter duration
        ///     sendAt timeOfDay
        ///     repeat [Every 2<sec|min|hrs|days|wks|mos> | Monthly (Day 1) | ...]
        ///     retry RetryPolicy 
        /// }
//        type RequesterBuilder internal () =
//            member _.Yield _ =
//                ()
//                
//        let request = RequesterBuilder()
        
        