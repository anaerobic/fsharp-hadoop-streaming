namespace FSharp.Hadoop.MapperXml

open System
open System.Configuration
open System.Collections.Generic
open System.Linq
open System.IO
open System.Text
open System.Xml
open System.Xml.Linq

open FSharp.Hadoop.MapReduce
open FSharp.Hadoop.Utilities
open FSharp.Hadoop.Utilities.Arguments

module Controller = 

    let Run (args:string array) =    
    
        // Define what arguments are expected
        let defs = [
            {ArgInfo.Command="input"; Description="Input XML File"; Required=false };
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
        let outputCollector ((outputKey1, outputKey2) , outputValue) =
            let output = sprintf "%s\t%s\t%O" outputKey1 outputKey2 outputValue
            writer.WriteLine(output)

        // Write an counter entry
        let counterReporter docType = 
            stderr.WriteLine (sprintf "reporter:counter:Elements Processed,%s,1" docType)

        let nodename = StoreXmlElementMapper.MapNode
        let nodestart = "<" + nodename + ">"
        let nodeend = "</" + nodename + ">"

        // Read the next line of the input stream
        let readLine() = 
            reader.ReadLine()      

        // Parse the input stream into a sequence of XElement types
        let elementBuilder = new StringBuilder(1024)
        let rec xmlElements inContent (someContent:string option) = seq {
            let line =
                match someContent with
                | Some(content) -> content
                | None -> readLine()

            if not (box line = null) then
                if (inContent) then
                    // Try to find the end element and yield accordingly
                    let offset = line.IndexOf(nodeend, 0, StringComparison.InvariantCultureIgnoreCase)
                    if (offset > -1) then
                        // Found the endnode so append and start new XElement
                        let content = line.Substring(0, offset + nodeend.Length)
                        elementBuilder.Append(content) |> ignore
                        let nextContent =                             
                            if (offset + nodeend.Length = line.Length) then
                                None
                            else
                                Some(line.Substring(offset + nodeend.Length))
                        yield XElement.Parse(elementBuilder.ToString())
                        elementBuilder.Clear() |> ignore
                        yield! xmlElements false nextContent
                    else
                        // Just a content line so append
                        elementBuilder.AppendLine(line) |> ignore
                        yield! xmlElements true None
                else
                    // Find the start node element and start building
                    let offset = line.IndexOf(nodestart, 0, StringComparison.InvariantCultureIgnoreCase)
                    if (offset > -1) then
                        yield! xmlElements true (Some(line.Substring(offset)))
                    else
                        yield! xmlElements false None            
        }        
        
        // Process the XElement sequence and report on successes
        let elementProcessed value = 
            outputCollector value
            counterReporter "Successfully Processed"

        try 
            xmlElements false None
            |> Seq.map StoreXmlElementMapper.Map
            |> Seq.iter (Seq.iter elementProcessed)
        with
        | :? System.Xml.XmlException ->
            // Ignore invalid xml elements but report on count
            counterReporter "Invalid Elements"

        // Close the streams
        reader.Close()
        writer.Close()