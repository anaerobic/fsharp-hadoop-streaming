namespace FSharp.Hadoop.ReducerXml

open System
open System.Configuration
open System.IO

open FSharp.Hadoop.MapReduce
open FSharp.Hadoop.Utilities
open FSharp.Hadoop.Utilities.Arguments

module Controller = 

    let Run (args:string array) = 

        // Define what arguments are expected
        let defs = [
            {ArgInfo.Command="input"; Description="Input File"; Required=false };
            {ArgInfo.Command="output"; Description="Output File"; Required=false } ]
 
        // Parse Arguments into a Dictionary
        let parsedArgs = Arguments.ParseArgs args defs

        // Ensure Standard Input/Output and allow for debug configuration
        use reader = 
            if parsedArgs.ContainsKey("input") then
                new StreamReader(Path.GetFullPath(parsedArgs.["input"]))
            else
                new StreamReader(Console.OpenStandardInput())

        use writer =
            if parsedArgs.ContainsKey("output") then
                new StreamWriter(Path.GetFullPath(parsedArgs.["output"]))
            else
                new StreamWriter(Console.OpenStandardOutput(), AutoFlush = true)
        
        // Combine the name/value output into a string
        let outputCollector outputKey1 outputKey2 outputValue =            
            let output = sprintf "%s\t%s\t%O" outputKey1 outputKey2 outputValue
            writer.WriteLine(output)

        // Read the next line of the input stream
        let readLine() = 
            reader.ReadLine()

        // Parse the input into the required name/value pair
        let parseLine (input:string) = 
            let keyValue = input.Split('\t')
            (keyValue.[0].Trim(), keyValue.[1].Trim(), keyValue.[2].Trim())

        // Converts a input line into an option
        let getInput() = 
            let input = readLine()
            if not(String.IsNullOrWhiteSpace(input)) then
                Some(parseLine input)
            else
                None

        // Creates a sequence of the input based on the provided key
        let lastInput = ref None
        let continueDo = ref false
        let inputsByKey key firstValue = seq {
            // Yield any value from previous read
            yield firstValue

            continueDo := true
            while !continueDo do                
                match getInput() with
                | Some(input) ->
                    let (key1, key2, value) = input
                    if key1 = (fst key) && key2 = (snd key) then
                        // Yield found value and remainder of sequence                    
                        yield value                    
                    else
                        // Have a value but different key
                        lastInput := Some(input)
                        continueDo := false
                | None ->
                    // Have no more entries
                    lastInput := None
                    continueDo := false
        }

        // Controls the calling of the reducer
        let rec processInput (input:(string*string*string) option) =
            if input.IsSome then
                let (key1, key2, value) = input.Value
                let key = (key1, key2)
                let reduced = StoreXmlElementReducer.Reduce key (inputsByKey key value)
                if reduced.IsSome then
                    outputCollector key1 key2 reduced.Value
                processInput lastInput.contents

        processInput (getInput())