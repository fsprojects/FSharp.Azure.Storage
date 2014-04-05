namespace DigitallyCreated.FSharp.Azure.IntegrationTests

open System
open DigitallyCreated.FSharp.Azure.TableStorage
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Xunit
open FsUnit.Xunit

module DataModification = 
    
    type GameSummary = 
        { Name: string
          Platform: string
          Developer : string }
        interface ITableIdentifiable with
            member g.GetIdentifier() = 
                { PartitionKey = g.Developer; RowKey = g.Name }

    type Game = 
        { [<RowKey>] Name: string
          Platform: string
          [<PartitionKey>] Developer : string
          HasMultiplayer: bool }


    type Tests() = 
    
        let account = CloudStorageAccount.Parse "UseDevelopmentStorage=true;"
        let tableClient = account.CreateCloudTableClient()
        let gameTableName = "TestsGame"
        let gameTable = tableClient.GetTableReference gameTableName

        let inTable = inTable tableClient
        let inTableAsync = inTableAsync tableClient

        do gameTable.DeleteIfExists() |> ignore
        do gameTable.Create() |> ignore

        let verifyGame game =
            let result = TableOperation.Retrieve(game.Developer, game.Name) |> gameTable.Execute
            let entity = result.Result :?> DynamicTableEntity
        
            entity.PartitionKey |> should equal game.Developer
            entity.RowKey |> should equal game.Name
            entity.Properties.["Name"].StringValue |> should equal game.Name
            entity.Properties.["Platform"].StringValue |> should equal game.Platform
            entity.Properties.["Developer"].StringValue |> should equal game.Developer
            entity.Properties.["HasMultiplayer"].BooleanValue |> should equal game.HasMultiplayer

        [<Fact>]
        let ``can insert a new record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let result = game |> insert |> inTable gameTableName

            result.HttpStatusCode |> should equal 204
            verifyGame game


        [<Fact>]
        let ``can insert a new record asynchronously`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let result = game |> insert |> inTableAsync gameTableName |> Async.RunSynchronously

            result.HttpStatusCode |> should equal 204
            verifyGame game


        [<Fact>]
        let ``fails when inserting a record that already exists`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> insert |> inTable gameTableName |> ignore
            (fun () -> game |> insert |> inTable gameTableName |> ignore) 
                |> should throw typeof<StorageException>


        [<Fact>]
        let ``can insert or replace a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> insert |> inTable gameTableName |> ignore

            let gameChanged = 
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            let result = gameChanged |> insertOrReplace |> inTable gameTableName
            result.HttpStatusCode |> should equal 204
            verifyGame gameChanged


        [<Fact>]
        let ``can force replace a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> insert |> inTable gameTableName |> ignore

            let gameChanged = 
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            let result = gameChanged |> forceReplace |> inTable gameTableName
            result.HttpStatusCode |> should equal 204
            verifyGame gameChanged


        [<Fact>]
        let ``can replace a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let originalResult = game |> insert |> inTable gameTableName

            let gameChanged = 
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            let result = (gameChanged, originalResult.Etag) |> replace |> inTable gameTableName
            result.HttpStatusCode |> should equal 204
            verifyGame gameChanged


        [<Fact>]
        let ``fails when replacing but etag is out of date`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let originalResult = game |> insert |> inTable gameTableName

            let gameChanged = 
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            (fun () -> (gameChanged, "bogus") |> replace |> inTable gameTableName |> ignore)
                |> should throw typeof<StorageException>


        [<Fact>]
        let ``can insert or merge a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> insert |> inTable gameTableName |> ignore

            let gameSummary = 
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            let result = gameSummary |> insertOrMerge |> inTable gameTableName
            result.HttpStatusCode |> should equal 204
            verifyGame { game with Platform = "PC" }


        [<Fact>]
        let ``can force merge a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> insert |> inTable gameTableName |> ignore

            let gameSummary = 
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            let result = gameSummary |> forceMerge |> inTable gameTableName
            result.HttpStatusCode |> should equal 204
            verifyGame { game with Platform = "PC" }


        [<Fact>]
        let ``can merge a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let originalResult = game |> insert |> inTable gameTableName

            let gameSummary = 
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            let result = (gameSummary, originalResult.Etag) |> merge |> inTable gameTableName
            result.HttpStatusCode |> should equal 204
            verifyGame { game with Platform = "PC" }


        [<Fact>]
        let ``fails when merging but etag is out of date`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> insert |> inTable gameTableName |> ignore

            let gameSummary = 
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            (fun () -> (gameSummary, "bogus") |> merge |> inTable gameTableName |> ignore) 
                |> should throw typeof<StorageException>