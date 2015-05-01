namespace FSharp.Hadoop.Reducer

open System

module Program =   

    [<EntryPoint>]
    let Main(args) = 

        Controller.Run args

        // main entry point return
        0
