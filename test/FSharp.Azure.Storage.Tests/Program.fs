module FSharp.Azure.Storage.Tests.Program

open System
open Expecto

[<EntryPoint>]
let main argv =
    let connStrEnvVar = Environment.GetEnvironmentVariable "FSHARP_AZURE_STORAGE_CONNECTION_STRING"
    let connectionString =
        if String.IsNullOrWhiteSpace connStrEnvVar
        then
            do printfn "Using Development Storage for tests"
            "UseDevelopmentStorage=true;"
        else
            do printfn "Using connection string from FSHARP_AZURE_STORAGE_CONNECTION_STRING env var for tests"
            connStrEnvVar

    runTestsWithArgs defaultConfig argv <|
        testList "FSharp.Azure.Storage" [
            Table.QueryExpressionTests.tests
            Table.DataModificationTests.tests connectionString
            Table.DataQueryTests.tests connectionString
        ]
