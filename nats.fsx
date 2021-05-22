#load "/Users/rob/OneDrive/Dev/F#/prelude.fsx"


#r "nuget:System.IO.Pipelines"
#r "nuget:TaskBuilder.fs"
#r "nuget:FParsec"


open System
open System.Collections.Generic
open System.IO
open System.IO.Pipelines
open System.Net
open System.Security.Cryptography.X509Certificates
open System.Text 
open FParsec


module NATS = 
    module Transport = 

        type DuplexPipe(input: PipeReader, output: PipeWriter) = 
            interface IDuplexPipe with 
                member this.Input with get() = input 
                member this.Output with get() = output 

        let createConnectionPair inputOptions outputOptions = 
            let input  = Pipe(inputOptions)
            let output = Pipe(outputOptions)

            let transportToApplication = DuplexPipe(output.Reader, input.Writer) 
            let applicationToTransport = DuplexPipe(input.Reader, output.Writer) 

            struct {| Transport = applicationToTransport; Application = transportToApplication|} 


    module Client =
        module Defaults =

            let Version              = Reflection.Assembly.GetExecutingAssembly().GetName().Version.ToString()
            [<Literal>]
            let Url                  = @"nats://localhost:4222"
            [<Literal>]
            let Port                 = 4222
            [<Literal>]
            let MaxReconnect         = 60
            [<Literal>]
            let ReconnectWait        = 2_000 
            [<Literal>]
            let ReconnectJitter      = 100
            [<Literal>]
            let ReconnectJitterTLS   = 1_000
            [<Literal>]
            let Timeout              = 2_000
            [<Literal>]
            let PingInterval         = 120_000
            [<Literal>]
            let MaxPingOut           = 2
            [<Literal>]
            let MaxChanLen           = 65_536
            [<Literal>]
            let ReconnectBufSize     = 8_388_608
            [<Literal>]
            let RequestChanLen       = 8
            [<Literal>]
            let DrainTimeout         = 30_000
            [<Literal>]
            let LangString           = "F#"

        module Constants =
            [<Literal>]
            let StaleConnection                 = "stale connection"
            [<Literal>]
            let PermissionsError                = "permissions violation"
            [<Literal>]
            let AuthorizationError              = "authorization violation"
            [<Literal>]
            let AuthenticationExpired           = "user authentication expired"
            [<Literal>]
            let AuthenticationRevoked           = "user authentication revoked"
            [<Literal>]
            let AccountAuthenticationExpired    = "account authentication expired"

        type Err = 
            | ErrConnectionClosed 
            | ErrConnectionDraining 
            | ErrDrainTimeout 
            | ErrConnectionReconnecting 
            | ErrSecureConnRequired 
            | ErrSecureConnWanted 
            | ErrBadSubscription 
            | ErrTypeSubscription 
            | ErrBadSubject 
            | ErrBadQueueName 
            | ErrSlowConsumer 
            | ErrTimeout 
            | ErrBadTimeout 
            | ErrAuthorization 
            | ErrAuthExpired 
            | ErrAuthRevoked 
            | ErrAccountAuthExpired 
            | ErrNoServers 
            | ErrJsonParse 
            | ErrChanArg 
            | ErrMaxPayload 
            | ErrMaxMessages 
            | ErrSyncSubRequired 
            | ErrMultipleTLSConfigs 
            | ErrNoInfoReceived 
            | ErrReconnectBufExceeded 
            | ErrInvalidConnection 
            | ErrInvalidMsg 
            | ErrInvalidArg 
            | ErrInvalidContext 
            | ErrNoDeadlineContext 
            | ErrNoEchoNotSupported 
            | ErrClientIDNotSupported 
            | ErrUserButNoSigCB 
            | ErrNkeyButNoSigCB 
            | ErrNoUserCB 
            | ErrNkeyAndUser 
            | ErrNkeysNotSupported 
            | ErrStaleConnection 
            | ErrTokenAlreadySet 
            | ErrMsgNotBound 
            | ErrMsgNoReply 
            | ErrClientIPNotSupported 
            | ErrDisconnected 
            | ErrHeadersNotSupported 
            | ErrBadHeaderMsg 
            | ErrNoResponders 
            | ErrNoContextOrTimeout 
            | ErrPullModeNotAllowed 
            | ErrJetStreamNotEnabled 
            | ErrJetStreamBadPre 
            | ErrNoStreamResponse 
            | ErrNotJSMessage 
            | ErrInvalidStreamName 
            | ErrInvalidDurableName 
            | ErrNoMatchingStream 
            | ErrSubjectMismatch 
            | ErrContextAndTimeout 
            | ErrInvalidJSAck 
            | ErrMultiStreamUnsupported 
            | ErrStreamNameRequired 
            | ErrConsumerConfigRequired 
            | ErrStreamSnapshotConfigRequired 
            | ErrDeliverSubjectRequired 
            | ErrParseError of Exception 

        let errAsString = function
            | ErrConnectionClosed             -> "nats: connection closed"
            | ErrConnectionDraining           -> "nats: connection draining"
            | ErrDrainTimeout                 -> "nats: draining connection timed out"
            | ErrConnectionReconnecting       -> "nats: connection reconnecting"
            | ErrSecureConnRequired           -> "nats: secure connection required"
            | ErrSecureConnWanted             -> "nats: secure connection not available"
            | ErrBadSubscription              -> "nats: invalid subscription"
            | ErrTypeSubscription             -> "nats: invalid subscription type"
            | ErrBadSubject                   -> "nats: invalid subject"
            | ErrBadQueueName                 -> "nats: invalid queue name"
            | ErrSlowConsumer                 -> "nats: slow consumer, messages dropped"
            | ErrTimeout                      -> "nats: timeout"
            | ErrBadTimeout                   -> "nats: timeout invalid"
            | ErrAuthorization                -> "nats: authorization violation"
            | ErrAuthExpired                  -> "nats: authentication expired"
            | ErrAuthRevoked                  -> "nats: authentication revoked"
            | ErrAccountAuthExpired           -> "nats: account authentication expired"
            | ErrNoServers                    -> "nats: no servers available for connection"
            | ErrJsonParse                    -> "nats: connect message, json parse error"
            | ErrChanArg                      -> "nats: argument needs to be a channel type"
            | ErrMaxPayload                   -> "nats: maximum payload exceeded"
            | ErrMaxMessages                  -> "nats: maximum messages delivered"
            | ErrSyncSubRequired              -> "nats: illegal call on an async subscription"
            | ErrMultipleTLSConfigs           -> "nats: multiple tls.Configs not allowed"
            | ErrNoInfoReceived               -> "nats: protocol exception, INFO not received"
            | ErrReconnectBufExceeded         -> "nats: outbound buffer limit exceeded"
            | ErrInvalidConnection            -> "nats: invalid connection"
            | ErrInvalidMsg                   -> "nats: invalid message or message nil"
            | ErrInvalidArg                   -> "nats: invalid argument"
            | ErrInvalidContext               -> "nats: invalid context"
            | ErrNoDeadlineContext            -> "nats: context requires a deadline"
            | ErrNoEchoNotSupported           -> "nats: no echo option not supported by this server"
            | ErrClientIDNotSupported         -> "nats: client ID not supported by this server"
            | ErrUserButNoSigCB               -> "nats: user callback defined without a signature handler"
            | ErrNkeyButNoSigCB               -> "nats: nkey defined without a signature handler"
            | ErrNoUserCB                     -> "nats: user callback not defined"
            | ErrNkeyAndUser                  -> "nats: user callback and nkey defined"
            | ErrNkeysNotSupported            -> "nats: nkeys not supported by the server"
            | ErrStaleConnection              -> "nats: " + Constants.StaleConnection
            | ErrTokenAlreadySet              -> "nats: token and token handler both set"
            | ErrMsgNotBound                  -> "nats: message is not bound to subscription/connection"
            | ErrMsgNoReply                   -> "nats: message does not have a reply"
            | ErrClientIPNotSupported         -> "nats: client IP not supported by this server"
            | ErrDisconnected                 -> "nats: server is disconnected"
            | ErrHeadersNotSupported          -> "nats: headers not supported by this server"
            | ErrBadHeaderMsg                 -> "nats: message could not decode headers"
            | ErrNoResponders                 -> "nats: no responders available for request"
            | ErrNoContextOrTimeout           -> "nats: no context or timeout given"
            | ErrPullModeNotAllowed           -> "nats: pull based not supported"
            | ErrJetStreamNotEnabled          -> "nats: jetstream not enabled"
            | ErrJetStreamBadPre              -> "nats: jetstream api prefix not valid"
            | ErrNoStreamResponse             -> "nats: no response from stream"
            | ErrNotJSMessage                 -> "nats: not a jetstream message"
            | ErrInvalidStreamName            -> "nats: invalid stream name"
            | ErrInvalidDurableName           -> "nats: invalid durable name"
            | ErrNoMatchingStream             -> "nats: no stream matches subject"
            | ErrSubjectMismatch              -> "nats: subject does not match consumer"
            | ErrContextAndTimeout            -> "nats: context and timeout can not both be set"
            | ErrInvalidJSAck                 -> "nats: invalid jetstream publish response"
            | ErrMultiStreamUnsupported       -> "nats: multiple streams are not supported"
            | ErrStreamNameRequired           -> "nats: stream name is required"
            | ErrConsumerConfigRequired       -> "nats: consumer configuration is required"
            | ErrStreamSnapshotConfigRequired -> "nats: stream snapshot configuration is required"
            | ErrDeliverSubjectRequired       -> "nats: deliver subject is required"
            | ErrParseError ex                -> sprintf $"nats: {ex.ToString()}"


        let private rand = Random(DateTime.UtcNow.Millisecond)

        [<RequireQualifiedAccess>]
        type Count<'a when 'a :> ValueType> = 
            | Some of x : 'a
            | Infinite

        type Duration = Milliseconds of x : uint

        type Options = {
            /// Represents a single NATS server url to which the client will be connecting.  
            /// If Servers option is also set, it then becomes the first server in the Servers array.
            Url : string

            /// A configured set of servers which this client will use when attempting to connect.
            Servers : string[]

            /// Configures whether we will randomize the server pool.
            NoRandomize : bool

            /// Configures whether the server will echo back messages that are sent 
            /// on this connection if we also have matching subscriptions.
            NoEcho : bool

            /// Optional name label which will be sent to the server on CONNECT to identify the client.
            Name : string

            /// Signals the server to send on OK ack for commands successfully processed by the server.
            Verbose : bool

            /// Signals the server whether is should be doing further validation of subjects.
            Pedantic : bool

            /// Enables TLS secure connections that skip server verification by default (NOT RECOMMENDED).
            Secure : bool

            /// Certificate store used for secure connections
            Certificates : X509Certificate2Collection

            /// Enables reconnection logic to be used when we encounter a disconnect from current server.
            AllowReconnect : bool

            /// <summary>
            /// Sets number of reconnect attempts. 
            /// </summary>
            /// <remarks>
            /// <para>In native NATS client a negative number indicates it will never give up.
            /// An F# type is used to here to make the intent more clear.</para>
            /// </remarks>
            MaxReconnect : Count<uint16>

            /// ReconnectWait sets the time to backoff after attempting a reconnect 
            /// to a server that we were already connected to previously.
            ReconnectWait : Duration

            /// <summary>
            /// ReconnectJitter sets the upper bound for a random delay added to ReconnectWait during a 
            /// reconnect when no TLS is used.
            /// </summary>
            /// <remarks> Note that any jitter is capped with ReconnectJitterMax. </remarks>
            ReconnectJitter : Duration

            /// <summary>
            /// ReconnectJitterTls sets the upper bound for a random delay added to ReconnectWait during a 
            /// reconnect when TLS is used.
            /// </summary>
            /// <remarks> Note that any jitter is capped with ReconnectJitterMax. </remarks>
            ReconnectJitterTls : Duration

            /// <summary>
            /// CustomReconnectDelayCallback is invoked after the library tried every 
            /// URL in the server list and failed to reconnect.  If no callback is provided, 
            /// the default is to use the sum of <see cref="ReconnectWait">ReconnectWait</see> and a random value between 
            /// zero and <see cref="ReconnectJitter">ReconnectJitter</see> or <see cref="ReconnectJitterTls">ReconnectJitterTls</see>.
            /// </summary>
            /// <remarks>
            /// It is strongly recommended that this value contains some jitter to prevent 
            /// all connections from attempting to reconnect at the same time.
            /// </remarks>
            CustomReconnectDelayCallback : Count<uint> -> Duration

            /// Timeout sets the timeout for a Dial operation on a connection 
            Timeout : Duration

            /// DrainTimeout sets teh timeout for a Drain Operation to complete
            DrainTimeout : Duration

            /// FlusherTimeout is the maximm time to wait for write operations to the underlying 
            /// connection to complete (including the flusher loop).
            FlusherTimeout : Duration

            /// PingInterval is the period at which the client will be sending ping commands to 
            /// the server, disabled if 0 or negative.
            PingInterval : Duration

            /// MaxPingOut is the maximum number of pending ping commands that can be awaiting a 
            /// response before raising a <see cref="ErrStaleConnection">Stale Connection</see> error.
            MaxPingOut : int

            /// ClosedCallback is the handler called when a client will no longer be connected.
            ClosedCallback : ConnectionHandler

            /// <summary>
            /// DiconnectedCallback is the handler called when the connection is disconnected.
            /// </summary>
            /// <remarks>
            /// Disconnected error could be nil; for instance, when user explicitly closees the connection.
            /// </remarks>
            DisconnectedCallback : ConnectionErrorHandler 

            /// ReconnectedCallback is the handler called when the connection is successfully reconnected.
            ReconnectedCallback : ConnectionHandler 

            /// DiscoveredServersCallback is the handler invoked when a new server has joined the cluster.
            DiscoveredServersCallback : ConnectionHandler

            /// AsyncErrorCallback is the async error handler (e.g. slow consumer errors).
            AsyncErrorCallback : AsyncErrorHandler

            /// ReconnectBufferSize is the size of the backing IO buffer during reconnect.  Once this has been 
            /// exhausted, publish operations will return an error.
            ReconnectBufferSize : uint64

            /// <summary>
            /// The size of the subscriber channel or number of messages the subscriber will buffer internally. 
            /// This impacts SyncSubscriptions only.
            /// </summary>
            /// <remarks>This does not affect AsyncSubscriptions which are dictated by PendingLimits()</remarks>
            SubscriberChannelLength : int

            /// UserJwtCallback represents the optional method that is used to fetch and return the account signed 
            /// JWT for this user.  Exceptions thrown here will be passed up to the caller when creating a connection.
            UserJwtCallback : UserJwtHandler 

            /// <summary>
            /// Nkey sets the public nkey that will be used to authenticate when connecting to the server.
            /// </summary>
            /// <remarks>
            /// UserJWT and Nkey are mutually exclusive and, if defined, UserJWT will take precedence.
            /// </remarks>
            Nkey : string

            /// SignatureCallback designates the function used to sign the none presented from the server.
            SignatureCallback : SignatureHandler

            /// Username to be used when connecting to a server.
            User : string

            /// Password to be used when connecting to a server.
            Password : string

            /// Token to be used when connecting to a server.
            Token : string

            /// AuTokenCallback is the handler invoked to generate the token used when connecting to a server.
            AuthenticationTokenCallback : AuthenticationTokenHandler

            /// Forces the old method of Requests that utilize a new Inbox and a new Subscription for each request.
            UseOldRequestStyle : bool

            /// Prevents the invocation of callbacks after Close() is called.  Clients won't receive notifications 
            /// when Close() is invoked by user code.  Default is to invoke the callbacks.
            NoCallbacksAfterClientClose : bool

            /// <summary>
            /// LameDuckModeHandler is the handler invoked when server notifies the connection that it has 
            /// entered lame duck mode.  That is, when the server gradually disconnects all its connections 
            /// before shutting down.
            /// </summary>
            /// <remarks>This is often used in deployments when upgrading NATS Servers.</remarks>
            LameDuckModeCallback : ConnectionHandler
        }
        and ConnectionHandler = Connection -> unit
        and ConnectionErrorHandler = Connection * Err -> unit
        and AsyncErrorHandler = Connection * Subscription * Err -> unit
        and UserJwtHandler = Subscription -> Result<string,Err>
        and SignatureHandler = ServerNonce -> Result<SignedNonce,Err>
        and ServerNonce = byte array
        and SignedNonce = byte array
        and AuthenticationTokenHandler = unit -> string
        and Connection = {
            Options : Options
            ClientIP : IPAddress
            ConnectedUrl : string
            ConnectedId : string
            Servers : string[]
            DiscoveredServers : string[]

            /// The last <see cref="Err"/> encountered by this instance, otherwise <c>None</c>.
            LastError : Err option

            /// The current <see cref="ConnectionState">state</see> of the <see cref="Connection">Connection</see>.
            ConnectionState : ConnectionState

            /// The <see cref="Statistics">Statistics</see> tracked for the <see cref="Connection">Connection</see>.
            Statistics : Statistics
            
            /// The maximum size in bytes of any payload sent to the connected NATS Server.
            MaxPayload : Count<uint64>

            /// The number of active subscriptions.
            SubscriptionCount : Count<uint>
        }
        /// State of the <see cref="Connection">Connection</see>
        and ConnectionState =
            /// The <see cref="Connection">Connection</see> is disconnected.
            | Disconnected 
            /// The <see cref="Connection">Connection</see> is connected to a NATS server.
            | Connected 
            /// The <see cref="Connection">Connection</see> has been closed.
            | Closed 
            /// The <see cref="Connection">Connection</see> is currently reconnecting.
            | Reconnecting 
            /// The <see cref="Connection">Connection</see> is currently connecting.
            | Connecting 
            /// The <see cref="Connection">Connection</see> is currently draining subscriptions.
            | DrainingSubscriptions
            /// The <see cref="Connection">Connection</see> is currently draining publishers.
            | DrainingPublishers
        and Statistics = {
            /// The number of inbound messages received.
            InboundMessages : uint64

            /// The number of outbound messages sent.
            OutboundMessages : uint64

            /// The number of incoming bytes.
            IncomingBytes : uint64

            /// The number of outgoing bytes.
            OutgoingBytes : uint64

            /// The number of reconnections.
            Reconnects : uint64
        }
        and Subscription = {
            /// <summary>
            /// Subject representing this subscription.  This can be different than the received subject inside
            /// a message if this is a wildcard.  See <em>remarks</em> for more.
            /// </summary>
            /// <remarks>
            /// <para>Subject names, including reply subject (INBOX) names, are case-sensitive
            /// and must be non-empty alphanumeric strings with no embedded whitespace, and optionally
            /// token-delimited using the dot character (<c>.</c>), e.g.: <c>FOO</c>, <c>BAR</c>,
            /// <c>foo.BAR</c>, <c>FOO.BAR</c>, and <c>FOO.BAR.BAZ</c> are all valid subject names, while:
            /// <c>FOO. BAR</c>, <c>foo. .bar</c> and <c>foo..bar</c> are <em>not</em> valid subject names.</para>
            /// <para>NATS supports the use of wildcards in subject subscriptions.</para>
            /// <list>
            /// <item>The asterisk character (<c>*</c>) matches any token at any level of the subject.</item>
            /// <item>The greater than symbol (<c>&gt;</c>), also known as the <em>full wildcard</em>, matches
            /// one or more tokens at the tail of a subject, and must be the last token. The wildcard subject
            /// <c>foo.&gt;</c> will match <c>foo.bar</c> or <c>foo.bar.baz.1</c>, but not <c>foo</c>.</item>
            /// <item>Wildcards must be separate tokens (<c>foo.*.bar</c> or <c>foo.&gt;</c> are syntactically
            /// valid; <c>foo*.bar</c>, <c>f*o.b*r</c> and <c>foo&gt;</c> are not).</item>
            /// </list>
            /// <para>For example, the wildcard subscriptions <c>foo.*.quux</c> and <c>foo.&gt;</c> both match
            /// <c>foo.bar.quux</c>, but only the latter matches <c>foo.bar.baz</c>. With the full wildcard,
            /// it is also possible to express interest in every subject that may exist in NATS (<c>&gt;</c>).</para>
            /// </remarks>
            Subject : string

            /// <summary>
            /// Optional queue group name.
            /// </summary>
            /// <remarks>
            /// If present, all subscriptions with the same name will form a distributed queue, and each message will only
            /// be processed by one member of the group. Although queue groups have multiple subscribers,
            /// each message is consumed by only one.
            /// </remarks>
            Queue : string

            /// The <see cref="Connection">Connection</see> associated with this instance.
            Connection : Connection

            /// Indicates whether or not the <see cref="Subscription">Subscription</see> is still valid.
            IsValid : bool

            /// The number of messages remaining in the delivery queue.
            QueueMessageCount : int
            
            /// <summary>
            /// The maximum allowed count of pending bytes.
            /// </summary>
            /// <value>The limit must not be zero (<c>0</c>). Negative values indicate there is no
            /// limit on the number of pending bytes.</value>
            PendingByteLimit : int

            /// <summary>
            /// The maximum allowed count of pending messages.
            /// </summary>
            /// <value>The limit must not be zero (<c>0</c>). Negative values indicate there is no
            /// limit on the number of pending messages.</value>
            PendingMessageLimit : Count<uint64>

            /// The number of bytes not yet processed on this instance.
            PendingBytes : uint64

            /// The number of messages not yet processed on this instance.
            PendingMessages : uint64

            /// The maximum number of pending bytes seen so far by this instance.
            MaxPendingBytes : uint64

            /// The maximum number of messages seen so far by this instance.
            MaxPendingMessages : uint64

            /// The number of delivered messages for this instance.
            Delivered : uint64

            /// <summary>
            /// The number of known dropped messages for this instance.
            /// </summary>
            /// <remarks>
            /// This will correspond to the messages dropped by violations of <see cref="PendingByteLimit">PendingByteLimit</see> 
            /// and/or <see cref="PendingMessageLimit">PendingMessageLimit</see>.  If the NATS server declares the connection a slow 
            /// consumer, the count may not be accurate.
            /// </remarks>
            Dropped : uint64
        }

        type Message = {
            Subject : string
            Reply : string
            Header : IReadOnlyDictionary<string,string>
            Data : byte[] option
            Subscription : Subscription
        }

        module Parser =

            [<Literal>]
            let OP_PLUS          = 1;
            [<Literal>]
            let OP_PLUS_O        = 2;
            [<Literal>]
            let OP_PLUS_OK       = 3;
            [<Literal>]
            let OP_MINUS         = 4;
            [<Literal>]
            let OP_MINUS_E       = 5;
            [<Literal>]
            let OP_MINUS_ER      = 6;
            [<Literal>]
            let OP_MINUS_ERR     = 7;
            [<Literal>]
            let OP_MINUS_ERR_SPC = 8;
            [<Literal>]
            let MINUS_ERR_ARG    = 9;
            [<Literal>]
            let OP_C             = 10;
            [<Literal>]
            let OP_CO            = 11;
            [<Literal>]
            let OP_CON           = 12;
            [<Literal>]
            let OP_CONN          = 13;
            [<Literal>]
            let OP_CONNE         = 14;
            [<Literal>]
            let OP_CONNEC        = 15;
            [<Literal>]
            let OP_CONNECT       = 16;
            [<Literal>]
            let CONNECT_ARG      = 17;
            [<Literal>]
            let OP_M             = 18;
            [<Literal>]
            let OP_MS            = 19;
            [<Literal>]
            let OP_MSG           = 20;
            [<Literal>]
            let OP_MSG_SPC       = 21;
            [<Literal>]
            let MSG_ARG          = 22;
            [<Literal>]
            let MSG_PAYLOAD      = 23;
            [<Literal>]
            let MSG_END          = 24;
            [<Literal>]
            let OP_P             = 25;
            [<Literal>]
            let OP_H             = 26;
            [<Literal>]
            let OP_PI            = 27;
            [<Literal>]
            let OP_PIN           = 28;
            [<Literal>]
            let OP_PING          = 29;
            [<Literal>]
            let OP_PO            = 30;
            [<Literal>]
            let OP_PON           = 31;
            [<Literal>]
            let OP_PONG          = 32;
            [<Literal>]
            let OP_I             = 33
            [<Literal>]
            let OP_IN            = 34
            [<Literal>]
            let OP_INF           = 35
            [<Literal>]
            let OP_INFO          = 36
            [<Literal>]
            let OP_INFO_SPC      = 37
            [<Literal>]
            let INFO_ARG         = 38

            type Message = { 
                subject: string 
                subscriptionId: string 
                replyTo: string option 
                payloadLength: int 
                payload: char[] }

            type ReceivedMessage = 
                | Info of string
                | Message of Message
                | Ping 
                | Pong 
                | Ok 
                | Error of string 
                | FatalError of string 
                // | HeaderMessage of char[]

            let achar ch = 
                let (A,a) = Char.ToUpper(ch), Char.ToLower(ch)
                pchar A <|> pchar a

            let parseProtocolCommand (command: string) =
                command.ToLower().ToCharArray()
                |> Seq.map (fun ch -> Char.ToUpper(ch), ch)
                |> Seq.map (fun (u,l) -> pchar u <|> pchar l)
                |> Seq.reduce (>>.)

            let spc : Parser<_,unit> = pchar ' ' <|> pchar '\t'
            let whitespace = many spc |>> ignore 

            let ok = parseProtocolCommand "+ok" .>> whitespace .>> newline >>% Ok

            let determineSeverity (s: string) = 
                if s.StartsWith("Invalid Subject") || s.StartsWith("Permissions") then Error (s.TrimEnd())
                else FatalError (s.TrimEnd()) 

            let err = parseProtocolCommand "-err" 
                      .>> spaces1 
                      >>. restOfLine false |>> determineSeverity
                      .>> newline 

            let info = parseProtocolCommand "info" 
                       .>> spaces1 
                       >>. restOfLine false
                       .>> newline
                       |>> Info

            let pingpong = achar 'p'
                           >>. (achar 'i' <|> achar 'o')
                           .>> achar 'n'
                           .>> achar 'g'
                           .>> whitespace
                           .>> newline
                           |>> (fun ch -> if ch = 'I' then Ping else Pong)
            
            let args p sep = 
                Inline.SepBy(stateFromFirstElement = (fun s -> [s]), 
                             foldState = (fun xs _ x -> x::xs), 
                             resultFromState = List.rev, 
                             elementParser = p, 
                             separatorParser = sep)

            let makeMsg (xs: string list) = 
                try
                    let create subj sid r cnt = 
                        { subject = subj; subscriptionId = sid; replyTo = r; payloadLength = Int32.Parse(cnt, Globalization.NumberStyles.Integer); payload = [| |]}

                    let respond msg = preturn msg 

                    match Seq.length xs with 
                    | 3 -> create xs.[0] xs.[1] None xs.[2] |> respond 
                    | 4 -> create xs.[0] xs.[1] (Some xs.[2]) xs.[3] |> respond 
                    | _ ->  fail "Invalid number of message (MSG) arguments.  \
                                  Arguments include <subject>, <sid>, [<replyTo>], <#bytes>, and <payload>."
                with 
                | :? ArgumentNullException as ex -> failFatally $"Unable to parse number of bytes in message (MSG) argument.\n{ex.ToString()}"
                | :? FormatException as ex -> failFatally $"Unable to recognize format of number of bytes in message (MSG) argument.\n{ex.ToString()}"
                | :? OverflowException as ex -> failFatally $"Number of bytes in message (MSG) is too large (max = 1,000,000).\n{ex.ToString()}"

            let nChars msg : Parser<Message,'u> = 
                fun stream -> 
                    let mutable ch = '\000'
                    let mutable i = 0
                    let arr = Array.zeroCreate<char> (msg.payloadLength)
                    while i < msg.payloadLength && ch <> EOS do 
                        ch <- stream.Read()
                        if ch = '\n' then stream.RegisterNewline()
                        if ch <> EOS then 
                            arr.[i] <- ch 
                            i <- i + 1
                    Reply ({ msg with payload = arr })

            let noDelims = isNoneOf [| ' '; '\t'; '\r'; '\n' |]
            let noWhitespace = anyOf [| ' '; '\t' |]
            let defaultMsg = { subject = ""; subscriptionId = ""; replyTo = None; payloadLength = 0; payload = [| |]}
            let msg = parseProtocolCommand "msg" 
                      .>> spaces1 
                      >>. args (manySatisfy noDelims) (many1 noWhitespace)
                      .>> newline 
                      >>= makeMsg
                      >>= nChars .>> newline 
                      |>> Message 


            let command : Parser<_,unit> = 
                choiceL [| ok; pingpong; err; info; msg |] "A protocol command (e.g. INFO, MSG, PING, PONG, +OK, -ERR)"

            [| "INFO { \"Key\": Value, }\r\n"; 
               "msg foo.1.bar.2 baz _INBOX.Test 11\r\nHello World\r\n"; 
               "MSG foo.1.bar.2 baz 12\r\nHello\r\nWorld\r\n"; 
               "PING\r\n"; 
               "pong\r\n"; 
               "pöNG\r\n"; 
               "ok\r\n"; 
               "+OK\r\n"; 
               "ERR\r\n"; 
               "-ERR This is a really bad error\r\n" |]
            |> Seq.iteri (fun i s -> printf $"\n{i+1,2}> {s}"; run command s |> printfn "    --> %A")


open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open FSharp.Control.Tasks
open FSharp.Control.Tasks.V2.ContextInsensitive


// Fetch the contents of a web page
let fetchUrl callback url =
    let req = WebRequest.Create(Uri(url))
    use resp = req.GetResponse()
    use stream = resp.GetResponseStream()
    use reader = new IO.StreamReader(stream)
    callback reader url

let fetchUrl2 callback url = 
    let req = WebRequest.Create(Uri(url))
    use resp = req.GetResponse()
    use stream = resp.GetResponseStream()
    callback stream url

let myCallback (reader:IO.StreamReader) url =
    let html = reader.ReadToEnd()
    let html1000 = html.Substring(0,1000)
    printfn "Downloaded %s. First 1000 is %s" url html1000
    html      // return all the html

let google = fetchUrl myCallback "http://google.com"

let lines : Parser<_,unit> = (sepBy (restOfLine false) newline) 

let pageLinesCallback (stream: Stream) url =  
    runParserOnStream lines () "web-page-lines" stream System.Text.Encoding.UTF8 
    |> (function | Success (xs, _, _) -> xs |> Seq.iter (printfn "%s") | Failure (s, perr, st) -> failwithf "%A: %s with user state %A" perr s st) 

let utf8 = fetchUrl2 pageLinesCallback "https://www.utf8-chartable.de/" 

exception NATSConnectionException of string 

let pipe = NATS.Transport.createConnectionPair PipeOptions.Default PipeOptions.Default 

let openTcp (url: Uri) = 
    task {
        if url.Scheme.ToLowerInvariant() <> "nats" then raise (NATSConnectionException $"Unable to connect to unknown scheme ({url.Scheme})")

        let addressFamily, dualMode = if Socket.OSSupportsIPv6 
                                      then AddressFamily.InterNetworkV6, true 
                                      else AddressFamily.InterNetwork, false 

        let client = new TcpClient(addressFamily) 
        client.Client.DualMode <- dualMode 

        let server = if url.IsDefaultPort then UriBuilder(url.Scheme, url.Host, 4222).Uri else url 

        use cancel = new CancellationTokenSource(2000)
        do! client.ConnectAsync(server.Host, server.Port, cancel.Token) 
        if cancel.IsCancellationRequested then raise (NATSConnectionException "timeout")
        client.NoDelay <- false 
        client.ReceiveBufferSize <- 64 * 1024 
        client.SendBufferSize <- 32 * 1024 
        return client
    }

task {
    try 
        use! client = Uri("nats://localhost:4222") |> openTcp
        
    
    with ex -> printfn $"{ex.ToString()}"
}
