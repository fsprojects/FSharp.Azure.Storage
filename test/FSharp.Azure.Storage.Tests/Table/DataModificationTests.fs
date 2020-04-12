module FSharp.Azure.Storage.Tests.Table.DataModificationTests

open System
open FSharp.Azure.Storage.Table
open Expecto
open Expecto.Flip
open FSharp.Azure.Storage.Tests
open Microsoft.Azure.Cosmos.Table

type GameWithOptions =
    { [<RowKey>] Name: string
      Platform: string option
      [<PartitionKey>] Developer : string
      HasMultiplayer: bool option }

type GameSummary =
    { Name: string
      Platform: string
      Developer : string }
    interface IEntityIdentifiable with
        member g.GetIdentifier() =
            { PartitionKey = g.Developer; RowKey = g.Name }

type Game =
    { [<RowKey>] Name: string
      Platform: string
      [<PartitionKey>] Developer : string
      HasMultiplayer: bool }

type PureRecord =
    { SuchPure : string
      VeryClean : string
      Wow : bool }

let getPureIdentifier p = { PartitionKey = p.SuchPure; RowKey = p.VeryClean }

type TypeWithSystemProps =
    { [<PartitionKey>] PartitionKey : string;
      [<RowKey>] RowKey : string;
      Timestamp : DateTimeOffset }

type TypeWithSystemPropsAttributes =
  { [<PartitionKey>] PartitionKey : string;
    [<RowKey>] RowKey : string;
    [<Timestamp>] Modified: DateTimeOffset option;
    [<Etag>] tag : string option }

type GameTableEntity() =
    inherit TableEntity()
    member val Name : string = null with get,set
    member val Platform : string = null with get,set
    member val Developer : string = null with get,set
    member val HasMultiplayer : bool = false with get,set

type NonTableEntityClass() =
    member val Name : string = null with get,set

type internal InternalRecord =
    { [<PartitionKey>] MuchInternal: string
      [<RowKey>] VeryWow: string }

type UnionProperty = A | B | C
type TypeWithUnionProperty =
    { [<PartitionKey>] PartitionKey : string;
      [<RowKey>] RowKey : string;
      UnionProp: UnionProperty; }

type EnumProperty = A = 1 | B = 2 | C = 3
type TypeWithEnumProperty =
    { [<PartitionKey>] PartitionKey : string;
      [<RowKey>] RowKey : string;
      EnumProp: EnumProperty; }

type UnionWithFieldProperty =
    | X of string
    | Y
type TypeWithUnionWithFieldProperty =
    { [<PartitionKey>] PartitionKey : string;
      [<RowKey>] RowKey : string;
      UnionWithFieldProp: UnionWithFieldProperty; }

type GameTempTable (tableClient) =
    inherit Storage.TempTable (tableClient)

    member this.InGameTable e = inTable tableClient this.Name e
    member this.InGameTableAsync e = inTableAsync tableClient this.Name e
    member this.InGameTableAsBatch e = inTableAsBatch tableClient this.Name e
    member this.InGameTableAsBatchAsync e = inTableAsBatchAsync tableClient this.Name e
    member this.VerifyGame game =
        let result = TableOperation.Retrieve(game.Developer, game.Name) |> this.Table.ExecuteAsync |> Async.AwaitTask |> Async.RunSynchronously
        let entity = result.Result :?> DynamicTableEntity

        entity.PartitionKey |> Expect.equal "PartitionKey is developer" game.Developer
        entity.RowKey |> Expect.equal "RowKey is name" game.Name
        entity.Properties.["Name"].StringValue |> Expect.equal "Name property is correct" game.Name
        entity.Properties.["Platform"].StringValue |> Expect.equal "Platform property is correct" game.Platform
        entity.Properties.["Developer"].StringValue |> Expect.equal "Developer property is correct" game.Developer
        entity.Properties.["HasMultiplayer"].BooleanValue |> Expect.equal "HasMultiplayer property is correct" (Nullable<bool> game.HasMultiplayer)
    member this.VerifyGameTableEntity (game : GameTableEntity) =
        let result = TableOperation.Retrieve(game.Developer, game.Name) |> this.Table.ExecuteAsync |> Async.AwaitTask |> Async.RunSynchronously
        let entity = result.Result :?> DynamicTableEntity

        entity.PartitionKey |> Expect.equal "PartitionKey is developer" game.Developer
        entity.RowKey |> Expect.equal "RowKey is name" game.Name
        entity.Properties.["Name"].StringValue |> Expect.equal "Name property is correct" game.Name
        entity.Properties.["Platform"].StringValue |> Expect.equal "Platform property is correct" game.Platform
        entity.Properties.["Developer"].StringValue |> Expect.equal "Developer property is correct" game.Developer
        entity.Properties.["HasMultiplayer"].BooleanValue |> Expect.equal "HasMultiplayer property is correct" (Nullable<bool> game.HasMultiplayer)

let tests connectionString =
    EntityIdentiferReader.GetIdentifier <- getPureIdentifier

    let account = CloudStorageAccount.Parse connectionString
    let tableClient = account.CreateCloudTableClient()

    let gameTestCase name testFn =
        testCase name (fun () ->
            use tempTable = new GameTempTable (tableClient)
            do testFn tempTable
        )

    let gameTestCaseAsync name testFn =
        testCaseAsync name (async {
            use tempTable = new GameTempTable (tableClient)
            do! testFn tempTable
        })

    testList "Data Modification Tests" [
        gameTestCase "can insert a new record" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let result = game |> Insert |> ts.InGameTable

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204
            ts.VerifyGame game

        gameTestCaseAsync "can insert a new record asynchronously" <| fun ts -> async {
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let! result = game |> Insert |> ts.InGameTableAsync

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204
            ts.VerifyGame game
        }

        gameTestCase "fails when inserting a record that already exists" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> ts.InGameTable |> ignore
            (fun () -> game |> Insert |> ts.InGameTable |> ignore)
                |> Expect.throwsT<StorageException> "Throws StorageException"

        gameTestCase "can insert or replace a record" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> ts.InGameTable |> ignore

            let gameChanged =
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            let result = gameChanged |> InsertOrReplace |> ts.InGameTable
            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204
            ts.VerifyGame gameChanged

        gameTestCase "can force replace a record" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> ts.InGameTable |> ignore

            let gameChanged =
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            let result = gameChanged |> ForceReplace |> ts.InGameTable
            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204
            ts.VerifyGame gameChanged

        gameTestCase "can replace a record" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let originalResult = game |> Insert |> ts.InGameTable

            let gameChanged =
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            let result = (gameChanged, originalResult.Etag) |> Replace |> ts.InGameTable
            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204
            ts.VerifyGame gameChanged

        gameTestCase "fails when replacing but etag is out of date" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> ts.InGameTable |> ignore

            let gameChanged =
                { game with
                    Platform = "PC"
                    HasMultiplayer = false }

            (fun () -> (gameChanged, "bogus") |> Replace |> ts.InGameTable |> ignore)
                |> Expect.throwsT<StorageException> "Throws StorageException"

        gameTestCase "can insert or merge a record" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> ts.InGameTable |> ignore

            let gameSummary =
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            let result = gameSummary |> InsertOrMerge |> ts.InGameTable
            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204
            ts.VerifyGame { game with Platform = "PC" }

        gameTestCase "can force merge a record" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> ts.InGameTable |> ignore

            let gameSummary =
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            let result = gameSummary |> ForceMerge |> ts.InGameTable
            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204
            ts.VerifyGame { game with Platform = "PC" }

        gameTestCase "can merge a record" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let originalResult = game |> Insert |> ts.InGameTable

            let gameSummary =
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            let result = (gameSummary, originalResult.Etag) |> Merge |> ts.InGameTable
            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204
            ts.VerifyGame { game with Platform = "PC" }

        gameTestCase "fails when merging but etag is out of date" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> ts.InGameTable |> ignore

            let gameSummary =
                { GameSummary.Name = game.Name
                  Platform = "PC"
                  Developer = game.Developer }

            (fun () -> (gameSummary, "bogus") |> Merge |> ts.InGameTable |> ignore)
                |> Expect.throwsT<StorageException> "Throws StorageException"

        gameTestCase "works when inserting a type that has properties that are system properties" <| fun ts ->
            //Note that Timestamp will be ignored by table storage
            let record = { PartitionKey = "TestPK"; RowKey = "TestRK"; Timestamp = DateTimeOffset.Now }

            let result = record |> Insert |> ts.InGameTable

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

        gameTestCase "can delete a record" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            let originalResult = game |> Insert |> ts.InGameTable

            let result = (game, originalResult.Etag) |> Delete |> ts.InGameTable
            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

            Query.all<Game>
            |> Query.where <@ fun _ s -> s.PartitionKey = game.Developer && s.RowKey = game.Name @>
            |> fromTable tableClient ts.Name
            |> Expect.isEmpty "Deleted row should not be found"

        gameTestCase "can force delete a record" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> ts.InGameTable |> ignore

            let result = game |> ForceDelete |> ts.InGameTable
            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

            Query.all<Game>
            |> Query.where <@ fun _ s -> s.PartitionKey = game.Developer && s.RowKey = game.Name @>
            |> fromTable tableClient ts.Name
            |> Expect.isEmpty "Deleted row should not be found"

        gameTestCase "can delete a record using only PK and RK" <| fun ts ->
            let game =
                { Name = "Halo 4"
                  Platform = "Xbox 360"
                  Developer = "343 Industries"
                  HasMultiplayer = true }

            game |> Insert |> ts.InGameTable |> ignore

            let result =
                { EntityIdentifier.PartitionKey = game.Developer; RowKey = game.Name }
                |> ForceDelete
                |> ts.InGameTable

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

            Query.all<Game>
            |> Query.where <@ fun _ s -> s.PartitionKey = game.Developer && s.RowKey = game.Name @>
            |> fromTable tableClient ts.Name
            |> Expect.isEmpty "Deleted row should not be found"

        gameTestCase "can insert a new table entity" <| fun ts ->
            let game =
                GameTableEntity (Name = "Halo 4",
                                 Platform = "Xbox 360",
                                 Developer = "343 Industries",
                                 HasMultiplayer = true,
                                 PartitionKey = "343 Industries",
                                 RowKey = "Halo 4")

            let result = game |> Insert |> ts.InGameTable

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204
            ts.VerifyGameTableEntity game

        gameTestCase "can replace a table entity" <| fun ts ->
            let game =
                GameTableEntity (Name = "Halo 4",
                                 Platform = "Xbox 360",
                                 Developer = "343 Industries",
                                 HasMultiplayer = true,
                                 PartitionKey = "343 Industries",
                                 RowKey = "Halo 4")

            let originalResult = game |> Insert |> ts.InGameTable

            do game.Platform <- "PC"
            do game.HasMultiplayer <- false
            do game.ETag <- null //This is to prove that replace will respect the etag you pass to it (below)

            let result = (game, originalResult.Etag) |> Replace |> ts.InGameTable
            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204
            ts.VerifyGameTableEntity game

        gameTestCase "inserting with types that aren't records or implement ITableEntity fails" <| fun ts ->
            (fun () -> NonTableEntityClass() |> Insert |> ts.InGameTable |> ignore)
                |> Expect.throwsT<Exception> "Throws Exception"

        gameTestCase "inserting many entities using autobatching works" <| fun ts ->
            let games =
                [seq { for i in 1 .. 120 ->
                        { Developer = "Valve"; Name = sprintf "Portal %i" i; Platform = "PC"; HasMultiplayer = true } };
                 seq { for i in 1 .. 150 ->
                        { Developer = "343 Industries"; Name = sprintf "Halo %i" i; Platform = "PC"; HasMultiplayer = true } }]
                |> Seq.concat
                |> Seq.toList

            let batches = games |> Seq.map Insert |> autobatch

            batches.Length |> Expect.equal "Correct number of batches" 4
            batches |> Seq.head |> List.length |> Expect.equal "First batch size correct" MaxBatchSize
            batches |> Seq.skip 1 |> Seq.head |> List.length |> Expect.equal "Second batch size correct" 20
            batches |> Seq.skip 2 |> Seq.head |> List.length |> Expect.equal "Third batch size correct" MaxBatchSize
            batches |> Seq.skip 3 |> Seq.head |> List.length |> Expect.equal "Fourth batch size correct" 50

            let results = batches |> List.map ts.InGameTableAsBatch

            results |> Seq.concat |> Seq.map (fun r -> r.HttpStatusCode) |> Expect.allEqual "All status codes equal 204" 204
            let readGames =
                Query.all<Game>
                |> fromTable tableClient ts.Name
                |> Seq.map fst
                |> Seq.toList
            readGames |> Expect.containsAll "All games were inserted" games
            readGames.Length |> Expect.equal "Number of games read equals inserted games" games.Length

        gameTestCaseAsync "inserting many entities asynchronously using autobatching works" <| fun ts -> async {
            let games =
                [seq { for i in 1 .. 120 ->
                        { Developer = "Valve"; Name = sprintf "Portal %i" i; Platform = "PC"; HasMultiplayer = true } };
                 seq { for i in 1 .. 150 ->
                        { Developer = "343 Industries"; Name = sprintf "Halo %i" i; Platform = "PC"; HasMultiplayer = true } }]
                |> Seq.concat
                |> Seq.toList

            let batches = games |> Seq.map Insert |> autobatch

            batches.Length |> Expect.equal "Correct number of batches" 4
            batches |> Seq.head |> List.length |> Expect.equal "First batch size correct" MaxBatchSize
            batches |> Seq.skip 1 |> Seq.head |> List.length |> Expect.equal "Second batch size correct" 20
            batches |> Seq.skip 2 |> Seq.head |> List.length |> Expect.equal "Third batch size correct" MaxBatchSize
            batches |> Seq.skip 3 |> Seq.head |> List.length |> Expect.equal "Fourth batch size correct" 50

            let! results =
                batches
                |> List.map ts.InGameTableAsBatchAsync
                |> Async.Parallel

            results |> Seq.concat |> Seq.map (fun r -> r.HttpStatusCode) |> Expect.allEqual "All status codes equal 204" 204
            let readGames =
                Query.all<Game>
                |> fromTable tableClient ts.Name
                |> Seq.map fst
                |> Seq.toList
            readGames |> Expect.containsAll "All games were inserted" games
            readGames.Length |> Expect.equal "Number of games read equals inserted games" games.Length
        }

        testCase "inserting two of the same entity with autobatching fails" <| fun () ->
            let games = [
                { Developer = "Valve"; Name = sprintf "Portal"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = sprintf "Portal"; Platform = "PC"; HasMultiplayer = false }
            ]

            (fun () -> games |> Seq.map Insert |> autobatch |> ignore)
                |> Expect.throwsT<Exception> "Throws Exception"

        gameTestCase "performing an operation on a type that uses a custom EntityIdentiferReader function works" <| fun ts ->
            let doge =
                { SuchPure = "MuchWin"
                  VeryClean = "SoShiny"
                  Wow = true }

            let result = doge |> Insert |> ts.InGameTable

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

        gameTestCase "inserting a record with option type fields works" <| fun ts ->
            let game =
                { GameWithOptions.Name = "Halo 4"
                  Platform = None
                  Developer = "343 Industries"
                  HasMultiplayer = None }

            let result = game |> Insert |> ts.InGameTable

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

        gameTestCase "inserting an internal record type works" <| fun ts ->
            let internalType =
                { MuchInternal = "SuchPrivates"
                  VeryWow = "Amaze" }

            let result = internalType |> Insert |> ts.InGameTable

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

        gameTestCase "inserting an record type with system attributes and no values works" <| fun ts ->
            let data =
                { TypeWithSystemPropsAttributes.PartitionKey = "Some Nice Partition Key"
                  RowKey = "Yup this is a row key"
                  Modified = None
                  tag = None }
            let result = data |> Insert |> ts.InGameTable

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

        gameTestCase "inserting an record type with system attributes and values works" <| fun ts ->
            let data =
                { TypeWithSystemPropsAttributes.PartitionKey = "Some Nice Partition Key"
                  RowKey = "Yup this is a row key"
                  Modified = Some DateTimeOffset.Now
                  tag = Some "Some tag" }
            let result = data |> Insert |> ts.InGameTable

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

        testCase "inserting a record type with union property works" <| fun () ->
            use ts = new Storage.TempTable (tableClient)
            let data = { PartitionKey = "PK"; RowKey = "RK"; UnionProp = A }

            let result = data |> Insert |> inTable tableClient ts.Name

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

        testCase "inserting a record type with enum property works" <| fun () ->
            use ts = new Storage.TempTable (tableClient)
            let data = { PartitionKey = "PK"; RowKey = "RK"; EnumProp = EnumProperty.A }

            let result = data |> Insert |> inTable tableClient ts.Name

            result.HttpStatusCode |> Expect.equal "Status code equals 204" 204

        testCase "inserting a record type with union that has fields property fails" <| fun () ->
            use ts = new Storage.TempTable (tableClient)
            let data = { PartitionKey = "PK"; RowKey = "RK"; UnionWithFieldProp = X("x") }

            (fun () -> data |> Insert |> inTable tableClient ts.Name |> ignore)
                |> Expect.throws "Union with field"
    ]
