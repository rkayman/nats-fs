namespace NATS

open System
open System.Buffers
open System.Runtime.CompilerServices
        

module Client =
    
    let foo = "The quick brown fox jumped over the fence."
    
    let bar start length (str: string) =
        str.AsSpan().Slice(start, length).ToString() |> printfn "%s"
        str.AsSpan().Slice(start, length).ToArray() |> printfn "%A"
        
    
    module Parser =
        
        module Constants = 

            [<Literal>]
            let internal CTRL_TAB  = 9uy
            [<Literal>]
            let internal CTRL_LF  = 10uy
            [<Literal>]
            let internal CTRL_CR  = 13uy
            [<Literal>]
            let internal SPACE  = 32uy
            [<Literal>]
            let internal PLUS  = 43uy
            [<Literal>]
            let internal MINUS = 45uy
            [<Literal>]
            let internal CH_E  = 69uy
            [<Literal>]
            let internal CH_e  = 101uy
            [<Literal>]
            let internal CH_F  = 70uy
            [<Literal>]
            let internal CH_f  = 102uy
            [<Literal>]
            let internal CH_G  = 71uy
            [<Literal>]
            let internal CH_g  = 103uy
            [<Literal>]
            let internal CH_H  = 72uy
            [<Literal>]
            let internal CH_h  = 104uy
            [<Literal>]
            let internal CH_I  = 73uy
            [<Literal>]
            let internal CH_i  = 105uy
            [<Literal>]
            let internal CH_K  = 75uy
            [<Literal>]
            let internal CH_k  = 107uy
            [<Literal>]
            let internal CH_M  = 77uy
            [<Literal>]
            let internal CH_m  = 109uy
            [<Literal>]
            let internal CH_N  = 78uy
            [<Literal>]
            let internal CH_n  = 110uy
            [<Literal>]
            let internal CH_O  = 79uy
            [<Literal>]
            let internal CH_o  = 111uy
            [<Literal>]
            let internal CH_P  = 80uy
            [<Literal>]
            let internal CH_p  = 112uy
            [<Literal>]
            let internal CH_R  = 82uy
            [<Literal>]
            let internal CH_r  = 114uy
            [<Literal>]
            let internal CH_S  = 83uy
            [<Literal>]
            let internal CH_s  = 115uy
        
        [<Struct>]
        type msgArg =
            { subject: byte[]
              reply:   byte[]
              sid:     int64
              hdr:     int
              size:    int }

        [<Literal>]
        let MAX_CONTROL_LINE_SIZE = 4096

        [<Struct>]
        type parseState =
            { state:   StateMachine
              as':     int
              drop:    int
              hdr:     int
              ma:      msgArg
              argBuf:  byte[] 
              msgBuf:  byte[]
              scratch: byte[] }
        
        and StateMachine =
            | OP_START          = 0
            | OP_PLUS           = 1
            | OP_PLUS_O         = 2
            | OP_PLUS_OK        = 3
            | OP_MINUS          = 4
            | OP_MINUS_E        = 5
            | OP_MINUS_ER       = 6
            | OP_MINUS_ERR      = 7
            | OP_MINUS_ERR_SPC  = 8
            | MINUS_ERR_ARG     = 9
            | OP_M              = 10
            | OP_MS             = 11
            | OP_MSG            = 12
            | OP_MSG_SPC        = 13
            | MSG_ARG           = 14
            | MSG_PAYLOAD       = 15
            | MSG_END           = 16
            | OP_H              = 17
            | OP_P              = 18
            | OP_PI             = 19
            | OP_PIN            = 20
            | OP_PING           = 21
            | OP_PO             = 22
            | OP_PON            = 23
            | OP_PONG           = 24
            | OP_I              = 25
            | OP_IN             = 26
            | OP_INF            = 27
            | OP_INFO           = 28
            | OP_INFO_SPC       = 29
            | INFO_ARG          = 30
        

        open Constants
        
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let inline private parseErr s b = $"nats: Parse Error {s}: '%s{b}'" 
  
        [<MethodImpl(MethodImplOptions.AggressiveOptimization)>]        
        let internal parse (ps: parseState) (buffer: ReadOnlySequence<byte>) =
            let mutable i = 0L
            let mutable b = 0uy
            let reader = SequenceReader(buffer)
            let mutable ps' = ps
            while not reader.End do
                if reader.TryRead(&b) then
                    match ps'.state with
                    | StateMachine.OP_START ->
                        match b with
                        | CH_M | CH_m ->
                            ps' <- { ps' with state = StateMachine.OP_M
                                              hdr = -1
                                              ma = { ps'.ma with hdr = -1 } }
                        | CH_H | CH_h ->
                            ps' <- { ps' with state = StateMachine.OP_H
                                              hdr = 0
                                              ma = { ps'.ma with hdr = 0 } }
                        | CH_P | CH_p -> 
                            ps' <- { ps' with state = StateMachine.OP_P }
                        | PLUS ->
                            ps' <- { ps' with state = StateMachine.OP_PLUS }
                        | MINUS ->
                            ps' <- { ps' with state = StateMachine.OP_MINUS }
                        | CH_I | CH_i ->
                            ps' <- { ps' with state = StateMachine.OP_I }
                        | _ -> reader.UnreadSpan.ToString() |> parseErr ps'.state |> Error 

                    | StateMachine.OP_H -> 
                        match b with
                        | CH_M | CH_m ->
                            ps' <- { ps' with state = StateMachine.OP_M }
                        | _ -> reader.UnreadSpan.ToString() |> parseErr ps'.state |> Error
                        
                    | StateMachine.OP_M -> 
                        match b with
                        | CH_S | CH_s ->
                            ps' <- { ps' with state = StateMachine.OP_MS }
                        | _ -> reader.UnreadSpan.ToString() |> parseErr ps'.state |> Error
                        
                    | StateMachine.OP_MS -> 
                        match b with
                        | CH_G | CH_g ->
                            ps' <- { ps' with state = StateMachine.OP_MSG }
                        | _ -> reader.UnreadSpan.ToString() |> parseErr ps'.state |> Error
                        
                    | StateMachine.OP_MSG -> 
                        match b with
                        | SPACE | CTRL_TAB ->
                            ps' <- { ps' with state = StateMachine.OP_MSG_SPC }
                        | _ -> reader.UnreadSpan.ToString() |> parseErr ps'.state |> Error
                        
                    | StateMachine.OP_MSG_SPC -> 
                        match b with
                        | SPACE | CTRL_TAB -> ()
                        | _ ->
                            ps' <- { ps' with state = StateMachine.MSG_ARG
                                              as' = buffer.GetOffset(reader.Position) |> int }

                    | StateMachine.MSG_ARG ->
                        match b with
                        | CTRL_CR ->
                            ps' <- { ps' with drop = 1 }
                        | CTRL_LF ->
                            let arg = if ps'.argBuf <> null then ps'.argBuf
                                      else 
                            
                    | StateMachine.MSG_PAYLOAD -> ()
                    | StateMachine.MSG_END -> ()
                    | StateMachine.OP_PLUS -> ()
                    | StateMachine.OP_PLUS_O -> ()
                    | StateMachine.OP_PLUS_OK -> ()
                    | StateMachine.OP_MINUS -> ()
                    | StateMachine.OP_MINUS_E -> ()
                    | StateMachine.OP_MINUS_ER -> ()
                    | StateMachine.OP_MINUS_ERR -> ()
                    | StateMachine.OP_MINUS_ERR_SPC -> ()
                    | StateMachine.MINUS_ERR_ARG -> ()
                    | StateMachine.OP_P -> ()
                    | StateMachine.OP_PI -> ()
                    | StateMachine.OP_PIN -> ()
                    | StateMachine.OP_PING -> ()
                    | StateMachine.OP_PO -> ()
                    | StateMachine.OP_PON -> ()
                    | StateMachine.OP_PONG -> ()
                    | StateMachine.OP_I -> ()
                    | StateMachine.OP_IN -> ()
                    | StateMachine.OP_INF -> ()
                    | StateMachine.OP_INFO -> ()
                    | StateMachine.OP_INFO_SPC -> ()
                    | StateMachine.INFO_ARG -> ()
                    | unknownState -> reader.UnreadSpan.ToString() |> parseErr ps'.state |> Error 
            
