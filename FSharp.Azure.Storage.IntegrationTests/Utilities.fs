namespace DigitallyCreated.FSharp.Azure.Storage.IntegrationTests

module Async =
    open DigitallyCreated.FSharp.Azure.Storage
    open DigitallyCreated.FSharp.Azure.Storage.Utilities

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
