hadoop.cmd jar lib/hadoop-streaming-ms.jar -input "/office/documents" -output "/office/authors" -mapper "..\..\jars\FSharp.Hadoop.MapperOffice.exe" -combiner "..\..\jars\FSharp.Hadoop.ReducerOffice.exe" -reducer "..\..\jars\FSharp.Hadoop.ReducerOffice.exe" -file "C:\Users\carlnol\Projects\FSharp.Hadoop.MapReduce\FSharp.Hadoop.MapperOffice\bin\Release\FSharp.Hadoop.MapperOffice.exe" -file "C:\Users\carlnol\Projects\FSharp.Hadoop.MapReduce\FSharp.Hadoop.ReducerOffice\bin\Release\FSharp.Hadoop.ReducerOffice.exe" -file "C:\Users\carlnol\Projects\FSharp.Hadoop.MapReduce\FSharp.Hadoop.MapReduce\bin\Release\FSharp.Hadoop.MapReduce.dll" -file "C:\Users\carlnol\Projects\FSharp.Hadoop.MapReduce\FSharp.Hadoop.Utilities\bin\Release\FSharp.Hadoop.Utilities.dll" -file "C:\Reference Assemblies\itextsharp.dll" -inputformat com.microsoft.hadoop.mapreduce.lib.input.BinaryDocumentWithNameInputFormat