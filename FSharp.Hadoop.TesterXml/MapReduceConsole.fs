namespace FSharp.HadoopTesterXml

open System
open System.IO
open System.Collections.Generic
open System.Diagnostics
open System.Globalization
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Xml
open System.Xml.Linq

open FSharp.Hadoop.Utilities
open FSharp.Hadoop.Utilities.Arguments

module MapReduceConsole =
        
    let Run args =

        // Define what arguments are expected
        let defs = [            
            {ArgInfo.Command="input"; Description="Input File"; Required=true };
            {ArgInfo.Command="output"; Description="Output File"; Required=true };
            {ArgInfo.Command="tempPath"; Description="Temp File Path"; Required=true };
            {ArgInfo.Command="mapper"; Description="Mapper EXE"; Required=true };
            {ArgInfo.Command="reducer"; Description="Reducer EXE"; Required=true };
            {ArgInfo.Command="nodename"; Description="XML Node"; Required=true }; ]

        // Parse Arguments into a Dictionary
        let parsedArgs = Arguments.ParseArgs args defs
        Arguments.DisplayArgs parsedArgs

        // define the executables
        let mapperExe = Path.GetFullPath(parsedArgs.["mapper"])
        let reducerExe = Path.GetFullPath(parsedArgs.["reducer"])
        let nodename = parsedArgs.["nodename"]
        let nodestart = "<" + nodename + ">"
        let nodeend = "</" + nodename + ">"

        Console.WriteLine()
        Console.WriteLine (sprintf "The Mapper file is:\t%O" mapperExe)
        Console.WriteLine (sprintf "The Reducer file is:\t%O" reducerExe)
        Console.WriteLine (sprintf "Processing Nodename:\t%O" nodename)

        // Get the file names
        let inputfile = Path.GetFullPath(parsedArgs.["input"])
        let outputfile = Path.GetFullPath(parsedArgs.["output"])

        let tempPath = Path.GetFullPath(parsedArgs.["tempPath"])
        let tempFile = Path.Combine(tempPath, Path.GetFileName(outputfile))

        let mappedfile = Path.ChangeExtension(tempFile, "mapped")
        let reducefile = Path.ChangeExtension(tempFile, "reduced")

        Console.WriteLine()
        Console.WriteLine (sprintf "The input file is:\t\t%O" inputfile)
        Console.WriteLine (sprintf "The mapped temp file is:\t%O" mappedfile)
        Console.WriteLine (sprintf "The reduced temp file is:\t%O" reducefile)
        Console.WriteLine (sprintf "The output file is:\t\t%O" outputfile)

        // Give the user an option to continue
        Console.WriteLine()
        Console.WriteLine("Hit ENTER to continue...")
        Console.ReadLine() |> ignore       

        // Parse the input stream into a sequence of XElement types


        let mapperProcess() = 
            use mapper = new Process()
            mapper.StartInfo.FileName <- mapperExe
            mapper.StartInfo.UseShellExecute <- false
            mapper.StartInfo.RedirectStandardInput <- true
            mapper.StartInfo.RedirectStandardOutput <- true
            mapper.Start() |> ignore

            use mapperInput = mapper.StandardInput
            use mapperOutput = mapper.StandardOutput 
        
            // Map the reader to a background thread so processing can happen in parallel
            Console.WriteLine "Mapper Processing Starting..."   

            let taskMapperFunc() = 
                use mapperWriter = File.CreateText(mappedfile)
                while not mapperOutput.EndOfStream do
                    mapperWriter.WriteLine(mapperOutput.ReadLine())
            let taskMapperWriting = Task.Factory.StartNew(Action(taskMapperFunc)) 

            // Pass the file into the mapper process and close input stream when done
            use mapperReader = new StreamReader(File.OpenRead(inputfile))
            let elementBuilder = new StringBuilder(1024)

            let rec xmlElements inContent (someContent:string option) = seq {
                let line =
                    match someContent with
                    | Some(content) -> content
                    | None -> mapperReader.ReadLine()

                if not (box line = null) then
                    if (inContent) then
                        // Try to find the end element and yield accordingly
                        let offset = line.IndexOf(nodeend, 0, StringComparison.InvariantCultureIgnoreCase)
                        if (offset > -1) then
                            // Found the endnode so always add a new line
                            let content = line.Substring(0, offset + nodeend.Length)
                            elementBuilder.AppendLine(content) |> ignore
                            let nextContent = 
                                if (offset + nodeend.Length = line.Length) then
                                    None
                                else
                                    Some(line.Substring(offset + nodeend.Length))
                            yield elementBuilder.ToString()
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

            xmlElements false None
            |> Seq.iter mapperInput.WriteLine

            mapperInput.Close()
            taskMapperWriting.Wait()
            mapperOutput.Close()

            mapper.WaitForExit()
            let result = match mapper.ExitCode with | 0 -> true | _ -> false

            mapper.Close()
            result

        // Sort the mapped file by the first field - mimic the role of Hadoop
        let hadoopProcess() = 
            Console.WriteLine "Hadoop Processing Starting..."

            let unsortedValues = seq {
                use reader = new StreamReader(File.OpenRead(mappedfile))
                while not reader.EndOfStream do
                    let input = reader.ReadLine()
                    let keyValue = input.Split('\t')
                    yield (keyValue.[0].Trim(), keyValue.[1].Trim(), keyValue.[2].Trim())
                reader.Close()
                }

            use writer = File.CreateText(reducefile)
            unsortedValues
            |> Seq.sortBy (fun (key1, key2, value) -> (key1, key2))
            |> Seq.iter (fun (key1, key2, value) -> writer.WriteLine (sprintf "%O\t%O\t%O" key1 key2 value))
            writer.Close()

        
        // Finally call the reducer process
        let reducerProcess() =
            use reducer = new Process()
            reducer.StartInfo.FileName <- reducerExe
            reducer.StartInfo.UseShellExecute <- false
            reducer.StartInfo.RedirectStandardInput <- true
            reducer.StartInfo.RedirectStandardOutput <- true
            reducer.Start() |> ignore

            use reducerInput = reducer.StandardInput
            use reducerOutput = reducer.StandardOutput 
        
            // Map the reader to a background thread so processing can happen in parallel
            Console.WriteLine "Reducer Processing Starting..." 

            let taskReducerFunc() = 
                use reducerWriter = File.CreateText(outputfile)
                while not reducerOutput.EndOfStream do
                    reducerWriter.WriteLine(reducerOutput.ReadLine())
            let taskReducerWriting = Task.Factory.StartNew(Action(taskReducerFunc)) 

            // Pass the file into the mapper process and close input stream when done
            use reducerReader = new StreamReader(File.OpenRead(reducefile))
            while not reducerReader.EndOfStream do
                reducerInput.WriteLine(reducerReader.ReadLine())

            reducerInput.Close()
            taskReducerWriting.Wait()
            reducerOutput.Close()

            reducer.WaitForExit()
            let result = match reducer.ExitCode with | 0 -> true | _ -> false

            reducer.Close()
            result


        // Finish test
        if mapperProcess() then
            Console.WriteLine "Mapper Processing Complete..."  

            hadoopProcess()
            Console.WriteLine "Hadoop Processing Complete..."

            if reducerProcess() then
                Console.WriteLine "Reducer Processing Complete..."

                Console.WriteLine "Processing Complete..."     
                   
        Console.ReadLine() |> ignore