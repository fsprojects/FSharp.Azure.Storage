namespace DigitallyCreated.FSharp.Azure.IntegrationTests

open DigitallyCreated.FSharp.Azure.TableStorage
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Xunit;
open FsUnit.Xunit;

type GameSummary = 
        { Name: string
          Platform: string
          Developer : string }
type Game = 
        { Name: string
          Platform: string
          Developer : string
          HasMultiplayer: bool }

type TableStorageTests() = 
    
    let account = CloudStorageAccount.Parse "UseDevelopmentStorage=true;"
    let tableClient = account.CreateCloudTableClient()
    let gameTableName = "TestsGame"
    let gameTable = tableClient.GetTableReference gameTableName

    let gameId g = { PartitionKey = g.Developer; RowKey = g.Name }
    let gameSummaryId (g : GameSummary) = { PartitionKey = g.Developer; RowKey = g.Name }

    let inTable = inTable tableClient

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

        let result = game |> insert gameId |> inTable gameTableName

        result.HttpStatusCode |> should equal 204
        verifyGame game


    [<Fact>]
    let ``fails when inserting a record that already exists`` () =
        let game = 
            { Name = "Halo 4"
              Platform = "Xbox 360"
              Developer = "343 Industries"
              HasMultiplayer = true }

        game |> insert gameId |> inTable gameTableName |> ignore
        (fun () -> game |> insert gameId |> inTable gameTableName |> ignore) 
            |> should throw typeof<StorageException>


    [<Fact>]
    let ``can insert or replace a record`` () =
        let game = 
            { Name = "Halo 4"
              Platform = "Xbox 360"
              Developer = "343 Industries"
              HasMultiplayer = true }

        game |> insert gameId |> inTable gameTableName |> ignore

        let gameChanged = 
            { game with
                Platform = "PC"
                HasMultiplayer = false }

        let result = gameChanged |> insertOrReplace gameId |> inTable gameTableName
        result.HttpStatusCode |> should equal 204
        verifyGame gameChanged


    [<Fact>]
    let ``can force replace a record`` () =
        let game = 
            { Name = "Halo 4"
              Platform = "Xbox 360"
              Developer = "343 Industries"
              HasMultiplayer = true }

        game |> insert gameId |> inTable gameTableName |> ignore

        let gameChanged = 
            { game with
                Platform = "PC"
                HasMultiplayer = false }

        let result = gameChanged |> forceReplace gameId |> inTable gameTableName
        result.HttpStatusCode |> should equal 204
        verifyGame gameChanged


    [<Fact>]
    let ``can replace a record`` () =
        let game = 
            { Name = "Halo 4"
              Platform = "Xbox 360"
              Developer = "343 Industries"
              HasMultiplayer = true }

        let originalResult = game |> insert gameId |> inTable gameTableName

        let gameChanged = 
            { game with
                Platform = "PC"
                HasMultiplayer = false }

        let result = (originalResult.Etag, gameChanged) |> replace gameId |> inTable gameTableName
        result.HttpStatusCode |> should equal 204
        verifyGame gameChanged


    [<Fact>]
    let ``fails when replacing but etag is out of date`` () =
        let game = 
            { Name = "Halo 4"
              Platform = "Xbox 360"
              Developer = "343 Industries"
              HasMultiplayer = true }

        let originalResult = game |> insert gameId |> inTable gameTableName

        let gameChanged = 
            { game with
                Platform = "PC"
                HasMultiplayer = false }

        (fun () ->("bogus", gameChanged) |> replace gameId |> inTable gameTableName |> ignore)
            |> should throw typeof<StorageException>


    [<Fact>]
    let ``can insert or merge a record`` () =
        let game = 
            { Name = "Halo 4"
              Platform = "Xbox 360"
              Developer = "343 Industries"
              HasMultiplayer = true }

        game |> insert gameId |> inTable gameTableName |> ignore

        let gameSummary = 
            { GameSummary.Name = game.Name
              Platform = "PC"
              Developer = game.Developer }

        let result = gameSummary |> insertOrMerge gameSummaryId |> inTable gameTableName
        result.HttpStatusCode |> should equal 204
        verifyGame { game with Platform = "PC" }


    [<Fact>]
    let ``can force merge a record`` () =
        let game = 
            { Name = "Halo 4"
              Platform = "Xbox 360"
              Developer = "343 Industries"
              HasMultiplayer = true }

        game |> insert gameId |> inTable gameTableName |> ignore

        let gameSummary = 
            { GameSummary.Name = game.Name
              Platform = "PC"
              Developer = game.Developer }

        let result = gameSummary |> forceMerge gameSummaryId |> inTable gameTableName
        result.HttpStatusCode |> should equal 204
        verifyGame { game with Platform = "PC" }


    [<Fact>]
    let ``can merge a record`` () =
        let game = 
            { Name = "Halo 4"
              Platform = "Xbox 360"
              Developer = "343 Industries"
              HasMultiplayer = true }

        let originalResult = game |> insert gameId |> inTable gameTableName

        let gameSummary = 
            { GameSummary.Name = game.Name
              Platform = "PC"
              Developer = game.Developer }

        let result = (originalResult.Etag, gameSummary) |> merge gameSummaryId |> inTable gameTableName
        result.HttpStatusCode |> should equal 204
        verifyGame { game with Platform = "PC" }


    [<Fact>]
    let ``fails when merging but etag is out of date`` () =
        let game = 
            { Name = "Halo 4"
              Platform = "Xbox 360"
              Developer = "343 Industries"
              HasMultiplayer = true }

        game |> insert gameId |> inTable gameTableName |> ignore

        let gameSummary = 
            { GameSummary.Name = game.Name
              Platform = "PC"
              Developer = game.Developer }

        (fun () -> ("bogus", gameSummary) |> merge gameSummaryId |> inTable gameTableName |> ignore) 
            |> should throw typeof<StorageException>