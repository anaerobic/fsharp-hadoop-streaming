namespace FSharp.Hadoop.MapReduce

open System

////type Mapper = 
////    static member Map (input:string) -> Some(key:string * value:obj)

// Extracts the QueryTime for each Platform Device
module MobilePhoneQueryMapper =

    // Performs the split into key/value
    let private splitInput (value:string) =
        try
            let splits = value.Split('\t')
            let devicePlatform = splits.[3]
            let queryTime = TimeSpan.Parse(splits.[1])
            Some(devicePlatform, queryTime)
        with
        | :? System.ArgumentException -> None


    // Map the data from input name/value to output name/value
    let Map (value:string) =
        splitInput value