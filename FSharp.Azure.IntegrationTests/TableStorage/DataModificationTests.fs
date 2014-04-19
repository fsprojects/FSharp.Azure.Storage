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

    type TypeWithSystemProps = 
        { [<PartitionKey>] PartitionKey : string; 
          [<RowKey>] RowKey : string; 
          Timestamp : DateTimeOffset }

    type GameTableEntity() = 
        inherit Microsoft.WindowsAzure.Storage.Table.TableEntity()
        member val Name : string = null with get,set
        member val Platform : string = null with get,set
        member val Developer : string = null with get,set
        member val HasMultiplayer : bool = false with get,set

    type NonTableEntityClass() =
        member val Name : string = null with get,set


    type Tests() = 
    
        let account = CloudStorageAccount.Parse "UseDevelopmentStorage=true;"
        let tableClient = account.CreateCloudTableClient()
        let gameTableName = "TestsGame"
        let gameTable = tableClient.GetTableReference gameTableName

        let inGameTable e = inTable tableClient gameTableName e
        let inGameTableAsync e = inTableAsync tableClient gameTableName e
        let inGameTableAsBatch e = inTableAsBatch tableClient gameTableName e
        let inGameTableAsBatchAsync e = inTableAsBatchAsync tableClient gameTableName e

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

        let verifyGameTableEntity (game : GameTableEntity) =
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

            let result = game |> Insert |> inGameTable

            result.HttpStatusCode |> should equal 204
            verifyGame game


        [<Fact>]
        let ``can insert a new record asynchronously`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let result = game |> Insert |> inGameTableAsync |> Async.RunSynchronously

            result.HttpStatusCode |> should equal 204
            verifyGame game


        [<Fact>]
        let ``fails when inserting a record that already exists`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> inGameTable |> ignore
            (fun () -> game |> Insert |> inGameTable |> ignore) 
                |> should throw typeof<StorageException>


        [<Fact>]
        let ``can insert or replace a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> inGameTable |> ignore

            let gameChanged = 
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            let result = gameChanged |> InsertOrReplace |> inGameTable
            result.HttpStatusCode |> should equal 204
            verifyGame gameChanged


        [<Fact>]
        let ``can force replace a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> inGameTable |> ignore

            let gameChanged = 
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            let result = gameChanged |> ForceReplace |> inGameTable
            result.HttpStatusCode |> should equal 204
            verifyGame gameChanged


        [<Fact>]
        let ``can replace a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let originalResult = game |> Insert |> inGameTable

            let gameChanged = 
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            let result = (gameChanged, originalResult.Etag) |> Replace |> inGameTable
            result.HttpStatusCode |> should equal 204
            verifyGame gameChanged


        [<Fact>]
        let ``fails when replacing but etag is out of date`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let originalResult = game |> Insert |> inGameTable

            let gameChanged = 
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            (fun () -> (gameChanged, "bogus") |> Replace |> inGameTable |> ignore)
                |> should throw typeof<StorageException>


        [<Fact>]
        let ``can insert or merge a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> inGameTable |> ignore

            let gameSummary = 
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            let result = gameSummary |> InsertOrMerge |> inGameTable
            result.HttpStatusCode |> should equal 204
            verifyGame { game with Platform = "PC" }


        [<Fact>]
        let ``can force merge a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> inGameTable |> ignore

            let gameSummary = 
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            let result = gameSummary |> ForceMerge |> inGameTable
            result.HttpStatusCode |> should equal 204
            verifyGame { game with Platform = "PC" }


        [<Fact>]
        let ``can merge a record`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let originalResult = game |> Insert |> inGameTable

            let gameSummary = 
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            let result = (gameSummary, originalResult.Etag) |> Merge |> inGameTable
            result.HttpStatusCode |> should equal 204
            verifyGame { game with Platform = "PC" }


        [<Fact>]
        let ``fails when merging but etag is out of date`` () =
            let game = 
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> inGameTable |> ignore

            let gameSummary = 
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            (fun () -> (gameSummary, "bogus") |> Merge |> inGameTable |> ignore) 
                |> should throw typeof<StorageException>


        [<Fact>]
        let ``works when inserting a type that has properties that are system properties``() =
            //Note that Timestamp will be ignored by table storage
            let record = { PartitionKey = "TestPK"; RowKey = "TestRK"; Timestamp = DateTimeOffset.Now }

            let result = record |> Insert |> inGameTable

            result.HttpStatusCode |> should equal 204


        [<Fact>]
        let ``can insert a new table entity`` () =
            let game = 
                GameTableEntity (Name = "Halo 4", 
                                 Platform = "Xbox 360", 
                                 Developer = "343 Industries",
                                 HasMultiplayer = true,
                                 PartitionKey = "343 Industries",
                                 RowKey = "Halo 4")

            let result = game |> Insert |> inGameTable

            result.HttpStatusCode |> should equal 204
            verifyGameTableEntity game


        [<Fact>]
        let ``can replace a table entity`` () =
            let game = 
                GameTableEntity (Name = "Halo 4", 
                                 Platform = "Xbox 360", 
                                 Developer = "343 Industries",
                                 HasMultiplayer = true,
                                 PartitionKey = "343 Industries",
                                 RowKey = "Halo 4")

            let originalResult = game |> Insert |> inGameTable

            do game.Platform <- "PC"
            do game.HasMultiplayer <- false
            do game.ETag <- null //This is to prove that replace will respect the etag you pass to it (below)

            let result = (game, originalResult.Etag) |> Replace |> inGameTable
            result.HttpStatusCode |> should equal 204
            verifyGameTableEntity game


        [<Fact>]
        let ``inserting with types that aren't records or implement ITableEntity fails``() = 
            (fun () -> NonTableEntityClass() |> Insert |> inGameTable |> ignore)
                |> should throw typeof<Exception>


        [<Fact>]
        let ``inserting many entities using autobatching works``() = 
            let games = 
                [seq { for i in 1 .. 120 -> 
                        { Developer = "Valve"; Name = sprintf "Portal %i" i; Platform = "PC"; HasMultiplayer = true } };
                 seq { for i in 1 .. 150 -> 
                        { Developer = "343 Industries"; Name = sprintf "Halo %i" i; Platform = "PC"; HasMultiplayer = true } }]
                |> Seq.concat
                |> Seq.toList

            let batches = games |> Seq.map Insert |> autobatch

            batches.Length |> should equal 4
            batches |> Seq.head |> List.length |> should equal MaxBatchSize
            batches |> Seq.skip 1 |> Seq.head |> List.length |> should equal 20
            batches |> Seq.skip 2 |> Seq.head |> List.length |> should equal MaxBatchSize
            batches |> Seq.skip 3 |> Seq.head |> List.length |> should equal 50

            let results = batches |> List.map inGameTableAsBatch

            results |> Seq.concat |> Seq.iter (fun r -> r.HttpStatusCode |> should equal 204)
            let readGames = 
                Query.all<Game> 
                |> fromTable tableClient gameTableName 
                |> Seq.map fst
                |> Seq.toList
            games |> Seq.iter (fun rg -> readGames |> List.exists (fun g -> g = rg) |> should equal true)
            readGames.Length |> should equal games.Length


        [<Fact>]
        let ``inserting many entities asynchronously using autobatching works``() = 
            let games = 
                [seq { for i in 1 .. 120 -> 
                        { Developer = "Valve"; Name = sprintf "Portal %i" i; Platform = "PC"; HasMultiplayer = true } };
                 seq { for i in 1 .. 150 -> 
                        { Developer = "343 Industries"; Name = sprintf "Halo %i" i; Platform = "PC"; HasMultiplayer = true } }]
                |> Seq.concat
                |> Seq.toList

            let batches = games |> Seq.map Insert |> autobatch

            batches.Length |> should equal 4
            batches |> Seq.head |> List.length |> should equal MaxBatchSize
            batches |> Seq.skip 1 |> Seq.head |> List.length |> should equal 20
            batches |> Seq.skip 2 |> Seq.head |> List.length |> should equal MaxBatchSize
            batches |> Seq.skip 3 |> Seq.head |> List.length |> should equal 50

            let results = 
                batches 
                |> List.map inGameTableAsBatchAsync
                |> Async.Parallel
                |> Async.RunSynchronously

            results |> Seq.concat |> Seq.iter (fun r -> r.HttpStatusCode |> should equal 204)
            let readGames = 
                Query.all<Game> 
                |> fromTable tableClient gameTableName 
                |> Seq.map fst
                |> Seq.toList
            games |> Seq.iter (fun rg -> readGames |> List.exists (fun g -> g = rg) |> should equal true)
            readGames.Length |> should equal games.Length


        [<Fact>]
        let ``inserting two of the same entity with autobatching fails``() = 
            let games = [
                { Developer = "Valve"; Name = sprintf "Portal"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = sprintf "Portal"; Platform = "PC"; HasMultiplayer = false }
            ]

            (fun () -> games |> Seq.map Insert |> autobatch |> ignore)
                |> should throw typeof<Exception>
            

