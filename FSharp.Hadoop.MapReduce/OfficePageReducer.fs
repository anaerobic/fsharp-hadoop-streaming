namespace FSharp.Hadoop.MapReduce

open System

////type Reducer = 
////    static member Reduce (key:string) -> (values:seq<string>) -> obj option

// Calculates the total pages per author
module OfficePageReducer = 

    let Reduce (key:string) (values:seq<string>) =
        let totalPages =
            values |> 
            Seq.fold (fun pages value -> pages + Int32.Parse(value)) 0

        Some(totalPages)