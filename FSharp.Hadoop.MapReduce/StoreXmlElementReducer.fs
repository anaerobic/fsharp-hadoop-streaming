namespace FSharp.Hadoop.MapReduce

open System

////type Reducer = 
////    static member Reduce (key:(string*string)) -> (values:seq<string>) -> Some(obj)

// Calculates the Total Revenue of the store demographics
module StoreXmlElementReducer = 

    let Reduce (key:(string*string)) (values:seq<string>) =
        let totalRevenue =
            values |> 
            Seq.fold (fun revenue value -> revenue + Int32.Parse(value)) 0

        Some(totalRevenue)