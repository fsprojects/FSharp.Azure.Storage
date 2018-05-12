module FSharp.Azure.Storage.Tests.Program

open System
open Expecto

[<EntryPoint>]
let main argv =
    let connStrEnvVar = Environment.GetEnvironmentVariable "FSharpAzureStorageConnectionString"
    let connectionString =
        if String.IsNullOrWhiteSpace connStrEnvVar
        then "UseDevelopmentStorage=true;"
        else connStrEnvVar

    runTestsWithArgs defaultConfig argv <|
        testList "FSharp.Azure.Storage" [
            Table.QueryExpressionTests.tests
            Table.DataModificationTests.tests connectionString
            Table.DataQueryTests.tests connectionString
        ]
