namespace FSharp.Azure.Storage.IntegrationTests

module ConnectionStrings =

    let storageEmulator = "UseDevelopmentStorage=true;"
    let fromEnvironment() = System.Environment.GetEnvironmentVariable "FSharpAzureStorageConnectionString"


module Storage =

    open System
    open Microsoft.WindowsAzure.Storage
    open Microsoft.WindowsAzure.Storage.Table

    let getTableName() = sprintf "TestTable%03d" (System.Random().Next(0,1000))
    
    let (|StorageException|_|) (e : exn) =
        match e with
        | :? StorageException as e -> Some e
        | :? AggregateException as e ->
            match e.InnerException with
            | :? StorageException as e -> Some e
            | _ -> None
        | _ -> None

    let clearTable (table : CloudTable) =
        let rec create () =
            try table.CreateIfNotExists()
            with StorageException e when e.RequestInformation.HttpStatusCode = 409 -> 
                System.Threading.Thread.Sleep 1000
                create ()

        ignore <| table.DeleteIfExists()
        ignore <| create ()


module Async =

    open FSharp.Azure.Storage
    open FSharp.Azure.Storage.Utilities  

    let Sequential computations =
        let rec innerSequential results computations =
           async {
                match computations with
                | current :: rest ->
                    let! result = current
                    return! rest |> innerSequential (result :: results)
                | [] -> 
                    return results
            }

        async {
            let! results = innerSequential [] computations
            return results |> List.rev
        }

    let ParallelByDegree degree computations = 
        if degree < 1 then invalidArg "degree" "degree must be 1 or greater" 
        let parallelWork = 
            computations 
            |> Seq.split degree 
            |> Seq.map (Seq.toList >> Sequential)
            |> Async.Parallel

        async {
            let! completedParallels = parallelWork
            return completedParallels |> Seq.concat |> Seq.toArray
        }
