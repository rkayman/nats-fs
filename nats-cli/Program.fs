open System
open System.IO.Pipelines

// Define a function to construct a message to print
let from whom = $"from %s{whom}"

[<EntryPoint>]
let main argv =
    let message = from "F#" // Call the function
    let pipe = Pipe() 
    printfn $"Hello world %s{message}"
    0 // return an integer exit code