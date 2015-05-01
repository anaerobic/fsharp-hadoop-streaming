namespace FSharp.Hadoop.Mapper

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
        let outputCollector (outputKey, outputValue) =
            let output = sprintf "%s\t%O" outputKey outputValue
            writer.WriteLine(output)

        // Read the next line of the input stream
        let readLine() = 
            reader.ReadLine()
            
        // Define the input sequence
        let rec inputs() = seq {
            let input = readLine()
            if not(String.IsNullOrWhiteSpace(input)) then
                // Yield the input and the remainder of the sequence
                yield input
                yield! inputs()
        }

        // Process the lines from the stream and pass into the mapper
        inputs()
        |> Seq.map MobilePhoneQueryMapper.Map
        |> Seq.filter Option.isSome
        |> Seq.iter (fun value -> outputCollector value.Value)       
        
        // Close the streams
        reader.Close()
        writer.Close()          
