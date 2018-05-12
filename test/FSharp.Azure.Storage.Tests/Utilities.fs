namespace FSharp.Azure.Storage.Tests

module Storage =

    open System
    open Microsoft.WindowsAzure.Storage.Table

    type TempTable (client : CloudTableClient) =
        let name = sprintf "TestTable%s" (Guid.NewGuid().ToString("N"))
        let table = client.GetTableReference name
        do table.CreateIfNotExistsAsync () |> Async.AwaitTask |> Async.Ignore |> Async.RunSynchronously

        member val Name = name
        member val Table = table

        interface IDisposable with
            member __.Dispose() =
                table.DeleteIfExistsAsync() |> Async.AwaitTask |> Async.Ignore |> Async.RunSynchronously

module Async =

    open FSharp.Azure.Storage

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
