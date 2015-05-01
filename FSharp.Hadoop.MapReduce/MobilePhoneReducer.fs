namespace FSharp.Hadoop.MapReduce

open System

////type Reducer = 
////    static member Reduce (key:string) -> (values:seq<string>) -> Some(obj)

// Calculates the (Min, Avg, Max) of the input stream query time (based on Platform Device)
module MobilePhoneQueryReducer = 

    let Reduce (key:string) (values:seq<string>) =
        let initState = (TimeSpan.MaxValue, TimeSpan.MinValue, 0L, 0L)
        let (minValue, maxValue, totalValue, totalCount) =
            values |> 
            Seq.fold (fun (minValue, maxValue, totalValue, totalCount) value ->
                (min minValue (TimeSpan.Parse(value)), max maxValue (TimeSpan.Parse(value)), totalValue + (int64)(TimeSpan.Parse(value).TotalSeconds), totalCount + 1L) ) initState

        Some(box (minValue, TimeSpan.FromSeconds((float)(totalValue/totalCount)), maxValue))


// Calculates the Max of the input stream query time (based on Platform Device)
module MobilePhoneQueryMaxReducer =

    let Reduce (key:string) (values:seq<string>) =
        match key with
        | "proprietary development" ->
            None
        | _ ->
            let maxValue =
                Seq.reduce (fun prev curr -> max prev curr) values
            Some(maxValue)