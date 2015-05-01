namespace FSharp.Hadoop.Utilities

open System
open System.IO

module ControllerNull = 

    let Run (args:string array) = 

        let rec inputs() =
            let input = Console.ReadLine()
            if not(String.IsNullOrWhiteSpace(input)) then
                Console.WriteLine input
                inputs()
        inputs()