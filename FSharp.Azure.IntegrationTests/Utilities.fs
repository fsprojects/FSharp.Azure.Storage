namespace DigitallyCreated.FSharp.Azure.IntegrationTests

module Array = 
    let window skip take (arr : 'a[]) =
        seq {
            for i in skip .. min (skip + take - 1) (arr.Length - 1) do yield arr.[i]
        }

module Async =
    let Sequential computations =
        async {
            let results = ref []
            for c in computations do
                let! result = c
                results := result :: !results
            return !results |> List.toArray |> Array.rev
        }


    let ParallelByDegree degree computations = 
        let computations = Array.ofSeq computations
        let windowSize = float(computations.Length) / float(degree) |> ceil |> int
        let splitSequentialWork = 
            seq { for i in 0 .. degree - 1 -> computations |> Array.window (i * windowSize) windowSize } 
            |> Seq.map Sequential
        let parallelWork = splitSequentialWork |> Async.Parallel
        async {
            let! completedParallels = parallelWork
            return completedParallels |> Seq.concat |> Seq.toArray
        }