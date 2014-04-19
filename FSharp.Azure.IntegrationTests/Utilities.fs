namespace DigitallyCreated.FSharp.Azure.IntegrationTests

module Async =
    open DigitallyCreated.FSharp.Azure

    let Sequential computations =
        async {
            let results = ref []
            for c in computations do
                let! result = c
                results := result :: !results
            return !results |> List.toArray |> Array.rev
        }


    let ParallelByDegree degree computations = 
        if degree < 1 then invalidArg "degree" "degree must be 1 or greater" 
        let computations = Array.ofSeq computations
        let windowSize = float(computations.Length) / float(degree) |> ceil |> int
        let degree = if windowSize = 1 then computations.Length else degree
        let splitSequentialWork = 
            seq { for i in 0 .. degree - 1 -> computations |> Array.window (i * windowSize) windowSize } 
            |> Seq.map Sequential
        let parallelWork = splitSequentialWork |> Async.Parallel
        async {
            let! completedParallels = parallelWork
            return completedParallels |> Seq.concat |> Seq.toArray
        }