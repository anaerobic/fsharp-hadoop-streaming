namespace FSharp.Hadoop.MapperOffice.Word

open System
open System.Configuration
open System.IO
open System.Text

open FSharp.Hadoop.MapReduce
open FSharp.Hadoop.Utilities
open FSharp.Hadoop.Utilities.Arguments

open DocumentFormat.OpenXml
open DocumentFormat.OpenXml.Packaging
open DocumentFormat.OpenXml.Wordprocessing

open iTextSharp.text
open iTextSharp.text.pdf

module Controller = 

    let Run (args:string array) =    
    
        // Define what arguments are expected
        let defs = [
            {ArgInfo.Command="input"; Description="Input Document"; Required=false };
            {ArgInfo.Command="output"; Description="Output File"; Required=false } ]
 
        // Parse Arguments into a Dictionary
        let parsedArgs = Arguments.ParseArgs args defs        
        
        // Ensure Standard Input/Output and allow for debug configuration
        let builder = new StringBuilder()
        let encoder = Text.UTF8Encoding()

        use reader = 
            if parsedArgs.ContainsKey("input") then
                builder.Append(Path.GetFileName(parsedArgs.["input"])) |> ignore
                File.Open(Path.GetFullPath(parsedArgs.["input"]), FileMode.Open) :> Stream
            else
                let stream = new MemoryStream()

                // Ignore bytes until one hits a tab
                let rec readTab() = 
                    let inByte = Console.OpenStandardInput().ReadByte()
                    if inByte <> 0x09 then                        
                        builder.Append(encoder.GetString([| (byte)inByte |])) |> ignore
                        readTab()
                readTab()

                // Copy the rest of the stream and truncate the last linefeed char
                Console.OpenStandardInput().CopyTo(stream)
                if (stream.Length > 0L) then
                    stream.SetLength(stream.Length - 1L)
                stream.Position <- 0L

                stream :> Stream

        use writer =
            if parsedArgs.ContainsKey("output") then
                new StreamWriter(Path.GetFullPath(parsedArgs.["output"]))
            else
                new StreamWriter(Console.OpenStandardOutput(), AutoFlush = true)
        
        let filename = builder.ToString()
        let (|WordDocument|PdfDocument|UnsupportedDocument|) extension = 
            if String.Equals(extension, ".docx", StringComparison.InvariantCultureIgnoreCase) then
                WordDocument
            else if String.Equals(extension, ".pdf", StringComparison.InvariantCultureIgnoreCase) then
                PdfDocument
            else
                UnsupportedDocument

        // Combine the name/value output into a string
        let outputCollector (outputKey, outputValue) =
            let output = sprintf "%s\t%O" outputKey outputValue
            writer.WriteLine(output)

        // Write an counter entry
        let counterReporter docType = 
            stderr.WriteLine (sprintf "reporter:counter:Documents Processed,%s,1" docType)
        
        // Check we do not have a null document
        if (reader.Length > 0L) then
            try
                match Path.GetExtension(filename) with
                | WordDocument ->
                    // Get access to the word processing document from the input stream
                    use document = WordprocessingDocument.Open(reader, false)
                    // Process the word document with the mapper
                    OfficeWordPageMapper.Map document
                    |> Seq.iter (fun value -> outputCollector value)        
                    // close document
                    document.Close()
                    counterReporter "Word Documents"
                | PdfDocument ->
                    // Get access to the pdf processing document from the input stream
                    let document = new PdfReader(reader)
                    // Process the word document with the mapper
                    OfficePdfPageMapper.Map document
                    |> Seq.iter (fun value -> outputCollector value)        
                    // close document
                    document.Close()
                    counterReporter "PDF Documents"
                | UnsupportedDocument ->
                    counterReporter "Unknown Document Types"
            with
            | :? System.IO.FileFormatException ->
                // Ignore invalid files formats
                counterReporter "Invalid Documents"
                ()

        // Close the streams
        reader.Close()
        writer.Close()    