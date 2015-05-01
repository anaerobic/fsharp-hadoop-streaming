namespace FSharp.Hadoop.MapReduce

open System

open iTextSharp.text
open iTextSharp.text.pdf

////type Mapper = 
////    static member Map (input:PdfReader) -> (Seq<key:string * value:obj>)

// Calculates the pages per author for a Pdf document
module OfficePdfPageMapper =

    let authorKey = "Author"
    let unknownAuthor = "unknown author"

    let getAuthors (document:PdfReader) =          
        // For PDF documents perform the split on a ","
        if document.Info.ContainsKey(authorKey) then
            let creators = document.Info.[authorKey]
            if String.IsNullOrWhiteSpace(creators) then
                [| unknownAuthor |]
            else
                creators.Split(',')
        else
            [| unknownAuthor |]

    let getPages (document:PdfReader) =
        // return page count
        document.NumberOfPages

    // Map the data from input name/value to output name/value
    let Map (document:PdfReader) =
        let pages = getPages document
        (getAuthors document)
        |> Seq.map (fun author -> (author, pages))