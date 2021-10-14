namespace Aspir.Messaging.Nats 

    open System
    open System.Collections.Generic
    open System.Runtime.CompilerServices
    open System.Reactive.Concurrency
    open System.Reactive.Linq
    open System.Threading.Tasks
    open FSharp.Control.Tasks.NonAffine
    open FsToolkit.ErrorHandling
    open NATS.Client
    open NATS.Client.Rx
    open Aspir.Utilities.Actor

    [<AutoOpen>]
    module Client =
        
        module Actor =

            exception SubscriptionKeyAlreadyExists of string 
            
            type NatsMessage =
                | Observed of Msg
                | ObservedEncoded of Msg * obj 

            and SubscriptionMessageHandler<'a> = ActorMessageHandler<NatsMessage,SubscriptionActorState<'a>>
            
            and SubscriptionErrorHandler<'a> = ActorErrorHandler<NatsMessage,SubscriptionActorState<'a>>
            
            and SubscriptionActorState<'a> =
                { parent : IActor<ConnectionMessage<'a>> option 
                  actorState : 'a option }
                
            and SubscriptionDetails<'a> =
                { subject: string
                  messageHandler: SubscriptionMessageHandler<'a>
                  errorHandler: SubscriptionErrorHandler<'a> option 
                  state: SubscriptionActorState<'a> }
            
            and ConnectionType =
                | Standard of IConnection
                | Encoded of conn: IConnection * serialize: (obj -> byte[]) * deserialize: (byte[] -> obj) 
                
            and ConnectionState<'a> =
                { kind: ConnectionType
                  subs: Dictionary<string,SubscriptionDetails<'a> * IDisposable * IActor<NatsMessage>> }
                
            and ConnectionMessage<'a> =
                | CreateSubscription of details: SubscriptionDetails<'a>
                                      * reply: TaskCompletionSource<IActor<NatsMessage>> 
                
            let private subscribe (conn: IConnection) subj (actor: IActor<_>) =
                conn.Observe(subj)
                    .ObserveOn(ThreadPoolScheduler.Instance)
                    .Select(Observed)
                    .Subscribe(actor.Tell)
                
            [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]                
            let internal connectionBody _ state msg =
                // TODO: Need to handle OnError/OnException and OnComplete for Subscribe
                vtask {
                    match msg with
                    | CreateSubscription (info, reply) ->
                        let conn, map = match state.kind with
                                        | Standard cn -> cn, Observed 
                                        | Encoded (cn, _, f) -> cn, (fun t -> ObservedEncoded(t, f t.Data)) 
                        if state.subs.ContainsKey(info.subject) then
                            let ex = SubscriptionKeyAlreadyExists($"[{info.subject}] already exists")
                            reply.SetException(ex)
                        else
                            let subActor = if Option.isNone info.errorHandler then
                                               Actor.Create(info.state, info.messageHandler)
                                           else
                                               Actor.Create(info.state, info.messageHandler, info.errorHandler.Value)
                            let sub = conn.Observe(info.subject)
                                          .ObserveOn(ThreadPoolScheduler.Instance)
                                          .Select(map)
                                          .Subscribe(subActor.Tell) 
                            state.subs.Add(info.subject, (info, sub, subActor))
                            reply.SetResult(subActor)
                        return state
                }
                
            let internal connectionError _ state _ ex =
                let conn = match state.kind with
                           | Standard cn -> cn
                           | Encoded (cn, _, _) -> cn 
                eprintfn $"[ERROR] Connection '{conn.Opts.Name}' encountered an exception:\n[DETAILS] %A{ex}"
                true 
                
            let internal createConnection (connType: ConnectionType) =
                let state = { kind = connType
                              subs = Dictionary<string,SubscriptionDetails<_> * IDisposable * IActor<_>>() }
                Actor.Create(state, connectionBody, connectionError) 
        
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

        type ConnectionBuilderState =
            { config: Options
              serializer: (obj -> byte[]) option
              deserializer: (byte[] -> obj) option }
        
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
        ///     encodeWith serializer deserializer
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
            let factory = ConnectionFactory()
            
            // TODO: Make tuple that includes a dictionary of KVs from the secret store
            member __.Yield _ =
                { config = ConnectionFactory.GetDefaultOptions()
                  serializer = None
                  deserializer = None } 

            [<CustomOperation("secretProvider")>]
            member __.SecretProvider(state, value) =
                // TODO: setup secret provider
                NotImplementedException("SecretProvider not yet implemented") |> raise 
                match value with
                | AzureKeyVault _ -> state
                | Custom _ -> state 
            
            [<CustomOperation("name")>]
            member __.Name(state, value) =
                // TODO: apply validation logic to value
                state.config.Name <- value
                state 
                
            [<CustomOperation("customInboxPrefix")>]
            member __.CustomInboxPrefix(state, value) =
                // TODO: apply validation logic to value
                state.config.CustomInboxPrefix <- value
                state
                    
            [<CustomOperation("verbose")>]
            member __.Verbose(state, value) =
                state.config.Verbose <- value
                state 
                
            [<CustomOperation("noEcho")>]
            member __.NoEcho(state, value) =
                state.config.NoEcho <- value
                state 
                
            [<CustomOperation("subscriberDeliveryTaskCount")>]
            member __.SubscriberDeliveryTaskCount(state, value) =
                state.config.SubscriberDeliveryTaskCount <- value
                state 
                
            [<CustomOperation("secure")>]
            member __.Secure(state, value) =
                state.config.Secure <- value
                state
                
            [<CustomOperation("user")>]
            member __.User(state, value) =
                state.config.User <- value
                state 
                
            [<CustomOperation("password")>]
            member __.Password(state, value) =
                state.config.Password <- value
                state 
                
            [<CustomOperation("token")>]
            member __.Token(state, value) =
                state.config.Token <- value
                state 
                
            [<CustomOperation("url")>]
            member __.Url(state, value) =
                // TODO: apply validation logic to value 
                state.config.Url <- value
                state 
                
            [<CustomOperation("servers")>]
            member __.Servers(state, value) =
                // TODO: apply validation logic to each item in value
                state.config.Servers <- value
                state 
                
            [<CustomOperation("userCredentials")>]
            member __.UserCredentials(state, value) =
                match value with
                | UseNKey (key, PrivateKey path)       -> state.config.SetNkey(key, path)
                | UseNKey (key, Callback handler)      -> state.config.SetNkey(key, handler)
                | UsePaths (credPath, None)            -> state.config.SetUserCredentials(credPath) 
                | UsePaths (credPath, Some pvtKeyPath) -> state.config.SetUserCredentials(credPath, pvtKeyPath) 
                | UseJwt (jwtEvent, sigEvent)          -> state.config.SetUserCredentialHandlers(jwtEvent, sigEvent) 
                state
                
            [<CustomOperation("encodeWith")>]
            member __.EncodeWith (state, serialize: obj -> byte[], deserialize: byte[] -> obj) =
                { state with serializer = Some serialize; deserializer = Some deserialize }
            
            [<CustomOperation("onDisconnected")>]
            member __.OnDisconnected(state, value) =
                let cfg = state.config
                let handler = EventHandler<ConnEventArgs>(value)
                cfg.DisconnectedEventHandler <- handler
                state 
                
            [<CustomOperation("onClosed")>]
            member __.OnClosed(state, value) =
                let cfg = state.config
                let handler = EventHandler<ConnEventArgs>(value)
                cfg.ClosedEventHandler <- handler
                state 

            [<CustomOperation("onReconnected")>]
            member __.OnReconnected(state, value) =
                let cfg = state.config
                let handler = EventHandler<ConnEventArgs>(value)
                cfg.ReconnectedEventHandler <- handler
                state 
                
            [<CustomOperation("onServerDiscovered")>]
            member __.OnServerDiscovered(state, value) =
                let cfg = state.config
                let handler = EventHandler<ConnEventArgs>(value)
                cfg.ServerDiscoveredEventHandler <- handler
                state
                
            [<CustomOperation("onLameDuckMode")>]
            member __.OnLameDuckMode(state, value) =
                let cfg = state.config
                let handler = EventHandler<ConnEventArgs>(value)
                cfg.LameDuckModeEventHandler <- handler
                state 

            [<CustomOperation("asyncErrorOccurred")>]
            member __.AsyncErrorOccurred(state, errHandler) =
                state.config.AsyncErrorEventHandler <- errHandler
                state
                
            [<CustomOperation("remoteCertValidator")>]
            member __.RemoteCertValidator(state, callback) =
                state.config.TLSRemoteCertificationValidationCallback <- callback
                state 

            member __.Run(state) =
                match state with
                | { config = cfg; serializer = None; deserializer = None } ->
                    let conn = factory.CreateConnection(cfg)
                    Actor.createConnection (Actor.Standard conn)

                | { config = cfg; serializer = Some s; deserializer = Some d } ->
                    let conn = factory.CreateConnection(cfg)
                    Actor.createConnection (Actor.Encoded (conn, s, d))

                | { config = _; serializer = Some _; deserializer = None } ->
                    failwith "Missing deserializer.  A deserializer is needed for the encoded connection"

                | { config = _; serializer = None; deserializer = Some _ } ->
                    failwith "Missing serializer.  A serializer is needed fr the encoded connection"
                
        let connect = ConnectionBuilder() 

        
        type SubscriptionBuilderState<'a> =
            { conn : IActor<Actor.ConnectionMessage<'a>> option
              topic : string option
              msgHandler : Actor.SubscriptionMessageHandler<'a> option
              errHandler : Actor.SubscriptionErrorHandler<'a> option
              initialState : Actor.SubscriptionActorState<'a> option }
        
        let private validateConnectionActor (actor: IActor<Actor.ConnectionMessage<_>> option) =
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
                    
        let private validateState (state: Actor.SubscriptionActorState<_> option) =
            match state with
            | None -> Error "Unexpected Error. SubscriptionActorState is not set.  Very likely, the connection has not been set either."
            | Some t -> Ok t 
                    
        let private validateMessageHandler (handler: Actor.SubscriptionMessageHandler<'State> option) =
            match handler with
            | None -> Error "The subscription actor requires a message handler to operate"
            | Some t -> Ok t
            
        let private validateSubscriptionBuilderState state =
            validation {
                let! conn = validateConnectionActor state.conn
                and! topic = validateTopic state.topic
                and! handler = validateMessageHandler state.msgHandler
                and! subState = validateState state.initialState 
                return {| cn = conn; subj = topic; msgh = handler; errh = state.errHandler; st = subState |}
            }
        
        /// ```fsharp
        /// 
        /// let sub = subscribe {
        ///     useConnection conn
        ///     topic "aspir.workflows.>"
        ///     messageHandler (NatsActorMessageHandler<'State>) 
        ///     errorHandler (NatsActorErrorHandler<'State>)
        ///     actorState (Some { foo = Foo; bar = Bar })
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
            
            member __.Yield _ =
                { conn = None
                  topic = None
                  msgHandler = None
                  errHandler = None
                  initialState = None }
                
            [<CustomOperation("withConnection")>]
            member __.WithConnection(state, value) =
                let st = state.initialState
                         |> Option.map (fun t -> { t with parent = Some value })
                         |> Option.orElse (Some { parent = Some value
                                                  actorState = None })
                { state with conn = Some value
                             initialState = st }
                
            [<CustomOperation("topic")>]
            member __.Topic(state, value) =
                { state with topic = Some value }
                
            [<CustomOperation("messageHandler")>]
            member __.MessageHandler(state, value) =
                { state with msgHandler = Some value }
                
            [<CustomOperation("errorHandler")>]
            member __.ErrorHandler(state, value) =
                { state with errHandler = Some value }
                
            [<CustomOperation("actorState")>]
            member __.ActorState(state, value) =
                let st = state.initialState
                         |> Option.map (fun t -> { t with actorState = value })
                         |> Option.orElse (Some { parent = None
                                                  actorState = value })
                { state with initialState = st }
                
            member __.Run(state) =
                match validateSubscriptionBuilderState state with
                | Error xs ->
                    failwith $"[Error] Unable to create subscription actor owing to the following errors:\n\t{xs}"
                | Ok x ->
                    let details = { Actor.subject = x.subj
                                    Actor.messageHandler = x.msgh
                                    Actor.errorHandler =  x.errh
                                    Actor.state = x.st }
                    x.cn.Ask(fun reply -> Actor.CreateSubscription (details, reply))
                
        let subscribe = SubscriptionBuilder() 

        
        type PublisherBuilderState<'a> =
            { conn : IActor<Actor.ConnectionMessage<'a>> option }
            
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
        type PublisherBuilder internal () =
            member __.Yield _ =
                { conn = None }
                
            [<CustomOperation("useConnection")>]
            member __.UseConnection(state, value) : PublisherBuilderState<_> =
                { state with conn = Some value }
                
                
        let publish = PublisherBuilder()
        
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
        type RequesterBuilder internal () =
            member __.Yield _ =
                ()
                
        let request = RequesterBuilder()
        
        