namespace FSharp.HadoopTesterXml

open System

module Program =   

    [<EntryPoint>]
    let Main(args) = 

        MapReduceConsole.Run args

        // main entry point return
        0
