namespace FSharp.Hadoop.MapReduce

open System
open System.Collections.Generic
open System.Linq
open System.IO
open System.Text
open System.Xml
open System.Xml.Linq

open DocumentFormat.OpenXml
open DocumentFormat.OpenXml.Packaging
open DocumentFormat.OpenXml.Wordprocessing

////type Mapper = 
////    static member Map (input:WordprocessingDocument) -> (Seq<key:string * value:obj>)

// Calculates the pages per author for a Word document
module OfficeWordPageMapper =

    let dc = XNamespace.Get("http://purl.org/dc/elements/1.1/")
    let cp = XNamespace.Get("http://schemas.openxmlformats.org/package/2006/metadata/core-properties")
    let unknownAuthor = "unknown author"

    let getAuthors (document:WordprocessingDocument) =
        let coreFilePropertiesXDoc = XElement.Load(document.CoreFilePropertiesPart.GetStream())
          
        // Take the first dc:creator element and split based on a ";"
        let creators = coreFilePropertiesXDoc.Elements(dc + "creator")
        if Seq.isEmpty creators then
            [| unknownAuthor |]
        else
            let creator = (Seq.head creators).Value
            if String.IsNullOrWhiteSpace(creator) then
                [| unknownAuthor |]
            else
                creator.Split(';')

    let getPages (document:WordprocessingDocument) =
        // return page count
        Int32.Parse(document.ExtendedFilePropertiesPart.Properties.Pages.Text)

    // Map the data from input name/value to output name/value
    let Map (document:WordprocessingDocument) =
        let pages = getPages document
        (getAuthors document)
        |> Seq.map (fun author -> (author, pages))
