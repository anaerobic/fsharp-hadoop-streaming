namespace FSharp.Hadoop.MapReduce

open System
open System.Collections.Generic
open System.Linq
open System.IO
open System.Text
open System.Xml
open System.Xml.Linq

////type Mapper = 
////    static member Map (input:XElement) -> Seq((string * string) * obj))

// Extracts the QueryTime for each Platform Device
module StoreXmlElementMapper = 
    
    let MapNode =
        "Store"

    let Map (element:XElement) =

        let aw = "http://schemas.microsoft.com/sqlserver/2004/07/adventure-works/StoreSurvey"

        let demographics = element.Element(XName.Get("Demographics")).Element(XName.Get("StoreSurvey", aw))

        if not(demographics = null) then
            let business = demographics.Element(XName.Get("BusinessType", aw)).Value
            let bank = demographics.Element(XName.Get("BankName", aw)).Value
            let key = (business, bank)

            let sales = Decimal.Parse(demographics.Element(XName.Get("AnnualSales", aw)).Value)

            Seq.singleton (key, sales)
        else
            Seq.empty