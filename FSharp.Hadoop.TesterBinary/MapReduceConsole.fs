namespace FSharp.HadoopTesterBinary

open System
open System.IO
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

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
            {ArgInfo.Command="reducer"; Description="Reducer EXE"; Required=true }; ]

        // Parse Arguments into a Dictionary
        let parsedArgs = Arguments.ParseArgs args defs
        Arguments.DisplayArgs parsedArgs

        // define the executables
        let mapperExe = Path.GetFullPath(parsedArgs.["mapper"])
        let reducerExe = Path.GetFullPath(parsedArgs.["reducer"])

        Console.WriteLine()
        Console.WriteLine (sprintf "The Mapper file is:\t%O" mapperExe)
        Console.WriteLine (sprintf "The Reducer file is:\t%O" reducerExe)

        // Get the file names
        let inputpath = Path.GetFullPath(parsedArgs.["input"])
        let outputfile = Path.GetFullPath(parsedArgs.["output"])

        let tempPath = Path.GetFullPath(parsedArgs.["tempPath"])
        let tempFile = Path.Combine(tempPath, Path.GetFileName(outputfile))

        let mappedfile = Path.ChangeExtension(tempFile, "mapped")
        let reducefile = Path.ChangeExtension(tempFile, "reduced")

        Console.WriteLine()
        Console.WriteLine (sprintf "The input path is:\t\t%O" inputpath)
        Console.WriteLine (sprintf "The mapped temp file is:\t%O" mappedfile)
        Console.WriteLine (sprintf "The reduced temp file is:\t%O" reducefile)
        Console.WriteLine (sprintf "The output file is:\t\t%O" outputfile)

        // Give the user an option to continue
        Console.WriteLine()
        Console.WriteLine("Hit ENTER to continue...")
        Console.ReadLine() |> ignore


        let CHUNK = 1024
        let buffer:byte array = Array.zeroCreate CHUNK

        // Call the mapper with the input file
        let mapperProcessLoop inputfile =
            use mapper = new Process()
            mapper.StartInfo.FileName <- mapperExe
            mapper.StartInfo.UseShellExecute <- false
            mapper.StartInfo.RedirectStandardInput <- true
            mapper.StartInfo.RedirectStandardOutput <- true
            mapper.Start() |> ignore

            use mapperInput = mapper.StandardInput.BaseStream
            use mapperOutput = mapper.StandardOutput 
        
            // Map the reader to a background thread so processing can happen in parallel
            let taskMapperFunc() = 
                use mapperWriter = File.AppendText(mappedfile)
                while not mapperOutput.EndOfStream do
                    mapperWriter.WriteLine(mapperOutput.ReadLine())
            let taskMapperWriting = Task.Factory.StartNew(Action(taskMapperFunc)) 

            // Pass the file into the mapper process and close input stream when done          
            use mapperReader = new BinaryReader(File.OpenRead(inputfile))
            let rec readLoop() = 
                let bytesread = mapperReader.Read(buffer, 0, CHUNK)
                if bytesread > 0 then
                    mapperInput.Write(buffer, 0, bytesread)
                    readLoop()

            // Write out a Filename/Tab/Document/LineFeed
            let filename = Path.GetFileName(inputfile)
            let encoding = Text.UTF8Encoding();
            mapperInput.Write(encoding.GetBytes(filename), 0, encoding.GetByteCount(filename))
            mapperInput.Write([| 0x09uy |], 0, 1)
            readLoop()
            mapperInput.Write([| 0x0Auy |], 0, 1)

            mapperInput.Close()
            taskMapperWriting.Wait()
            mapperOutput.Close()

            mapper.WaitForExit()
            let result = match mapper.ExitCode with | 0 -> true | _ -> false

            mapper.Close()
            result
      
        let mapperProcess() = 
            Console.WriteLine "Mapper Processing Starting..."  

            // Create the output file
            if File.Exists(mappedfile) then File.Delete(mappedfile)
            use mapperWriter = File.CreateText(mappedfile)
            mapperWriter.Close()

            // function to determine if valid document extension
            let isValidFile inputfile = 
                let extension = Path.GetExtension(inputfile)
                if String.Equals(extension, ".docx", StringComparison.InvariantCultureIgnoreCase) ||
                   String.Equals(extension, ".pdf", StringComparison.InvariantCultureIgnoreCase) then
                    true
                else
                    false

            // Process the files in the directory
            Directory.GetFiles(inputpath)
            |> Array.filter isValidFile
            |> Array.fold (fun result inputfile -> result && (mapperProcessLoop inputfile)) true

        // Sort the mapped file by the first field - mimic the role of Hadoop
        let hadoopProcess() = 
            Console.WriteLine "Hadoop Processing Starting..."

            let unsortedValues = seq {
                use reader = new StreamReader(File.OpenRead(mappedfile))
                while not reader.EndOfStream do
                    let input = reader.ReadLine()
                    let keyValue = input.Split('\t')
                    yield (keyValue.[0].Trim(), keyValue.[1].Trim())
                reader.Close()
                }

            use writer = File.CreateText(reducefile)
            unsortedValues
            |> Seq.sortBy fst
            |> Seq.iter (fun (key, value) -> writer.WriteLine (sprintf "%O\t%O" key value))
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