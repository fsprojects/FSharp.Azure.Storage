module FSharp.Azure.Storage.Tests.Table.DataQueryTests

open System
open FSharp.Azure.Storage.Table
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Expecto
open Expecto.Flip
open FSharp.Azure.Storage.Tests

type GameWithDateTime =
    { [<RowKey>] Name: string
      DevelopmentDate: DateTime
      DevelopmentDateAsOffset: DateTimeOffset
      [<PartitionKey>] Developer : string }

type internal InternalGame =
    { [<RowKey>] Name: string
      [<PartitionKey>] Developer: string }

type GameWithOptions =
    { Name: string
      Platform: string
      Developer : string
      HasMultiplayer: bool option
      Notes : string option }

      interface IEntityIdentifiable with
        member g.GetIdentifier() =
            { PartitionKey = g.Developer; RowKey = g.Name + "-" + g.Platform }

type GameWithUri =
    { Name: string
      Developer : string
      HasMultiplayer: bool
      Website : Uri }

      interface IEntityIdentifiable with
        member g.GetIdentifier() =
            { PartitionKey = g.Developer; RowKey = g.Name}
type GameWithUriOptions =
    { Name: string
      Developer : string
      Website: Uri option}

      interface IEntityIdentifiable with
        member g.GetIdentifier() =
            { PartitionKey = g.Developer; RowKey = g.Name }
type Game =
    { Name: string
      Platform: string
      Developer : string
      HasMultiplayer: bool }

      interface IEntityIdentifiable with
        member g.GetIdentifier() =
            { PartitionKey = g.Developer; RowKey = g.Name + "-" + g.Platform }

type TypeWithSystemProps =
    { [<PartitionKey>] PartitionKey : string;
      [<RowKey>] RowKey : string;
      Timestamp : DateTimeOffset }


type TypeWithSystemPropsAttributes =
    { [<PartitionKey>] PartitionKey : string;
      [<RowKey>] RowKey : string;
      [<Timestamp>] Modified: DateTimeOffset option;
      [<Etag>] tag : string option }

type Simple = { [<PartitionKey>] PK : string; [<RowKey>] RK : string }

type NonTableEntityClass() =
    member val Name : string = null with get,set

type GameTableEntity() =
    inherit Microsoft.WindowsAzure.Storage.Table.TableEntity()
    member val Name : string = null with get,set
    member val Platform : string = null with get,set
    member val Developer : string = null with get,set
    member val HasMultiplayer : bool = false with get,set

    override this.Equals other =
        match other with
        | :? Game as game ->
            this.Name = game.Name && this.Platform = game.Platform &&
            this.Developer = game.Developer && this.HasMultiplayer = game.HasMultiplayer
        | :? GameTableEntity as game ->
            this.Name = game.Name && this.Platform = game.Platform &&
            this.Developer = game.Developer && this.HasMultiplayer = game.HasMultiplayer &&
            this.PartitionKey = game.PartitionKey && this.RowKey = game.RowKey &&
            this.Timestamp = game.Timestamp && this.ETag = game.ETag
        | _ -> false

    override this.GetHashCode() =
        [box this.Name; box this.Platform; box this.Developer
         box this.HasMultiplayer; box this.PartitionKey;
         box this.RowKey; box this.Timestamp; box this.HasMultiplayer ]
            |> Seq.choose (fun o -> match o with | null -> None | o -> Some (o.GetHashCode()))
            |> Seq.reduce (^^^)

type GameTableEntityWithIgnoredProperty() =
    inherit Microsoft.WindowsAzure.Storage.Table.TableEntity()
    member val Name : string = null with get,set
    member val Platform : string = null with get,set
    member val Developer : string = null with get,set
    [<IgnoreProperty>]
    member val HasMultiplayer : bool = false with get,set

    override this.Equals other =
        match other with
        | :? Game as game ->
            this.Name = game.Name && this.Platform = game.Platform &&
            this.Developer = game.Developer //Don't compare HasMultiplayer because we're expecting it to not be read back from table storage
        | :? GameTableEntity as game ->
            this.Name = game.Name && this.Platform = game.Platform &&
            this.Developer = game.Developer && this.HasMultiplayer = game.HasMultiplayer &&
            this.PartitionKey = game.PartitionKey && this.RowKey = game.RowKey &&
            this.Timestamp = game.Timestamp && this.ETag = game.ETag
        | _ -> false

    override this.GetHashCode() =
        [box this.Name; box this.Platform; box this.Developer
         box this.HasMultiplayer; box this.PartitionKey;
         box this.RowKey; box this.Timestamp; box this.HasMultiplayer ]
            |> Seq.choose (fun o -> match o with | null -> None | o -> Some (o.GetHashCode()))
            |> Seq.reduce (^^^)

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
type TypeWithUnionWithFeidlProperty =
    { [<PartitionKey>] PartitionKey : string;
      [<RowKey>] RowKey : string;
      UnionWithFieldProp: UnionWithFieldProperty; }
  

let private processInParallel tableClient tableName operation =
    Seq.map operation
    >> autobatch
    >> Seq.map (inTableAsBatchAsync tableClient tableName)
    >> Async.ParallelByDegree 4
    >> Async.RunSynchronously
    >> Seq.concat

let private insertInParallelAndCheckSuccess tableClient tableName =
    processInParallel tableClient tableName Insert
    >> Seq.map (fun r -> r.HttpStatusCode)
    >> Expect.allEqual "All inserts have status code 204" 204

let private verifyMetadata metadata =
    metadata |> Seq.iter (fun (_, m) ->
        m.Etag |> Expect.isNotEmpty "Etag should not be null or empty"
        m.Timestamp |> Expect.notEqual "Timestamp should not be default value" (DateTimeOffset())
    )

let verifyRecords expected actual =
    actual |> Array.length |> Expect.equal "Record count should match" (expected |> Array.length)
    let actualRecords = actual |> Seq.map fst
    actualRecords |> Expect.all "All records match expected records" (fun a -> expected |> Seq.exists (fun e -> a.Equals(e)))

type GameTempTable (tableClient) =
    inherit Storage.TempTable (tableClient)

    static let data = [
        { Developer = "343 Industries"; Name = "Halo 4"; Platform = "Xbox 360"; HasMultiplayer = true }
        { Developer = "Bungie"; Name = "Halo 3"; Platform = "Xbox 360"; HasMultiplayer = true }
        { Developer = "Bungie"; Name = "Halo 2"; Platform = "Xbox 360"; HasMultiplayer = true }
        { Developer = "Bungie"; Name = "Halo 2"; Platform = "PC"; HasMultiplayer = true }
        { Developer = "Bungie"; Name = "Halo 1"; Platform = "Xbox 360"; HasMultiplayer = true }
        { Developer = "Bungie"; Name = "Halo 1"; Platform = "PC"; HasMultiplayer = true }
        { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
        { Developer = "Valve"; Name = "Portal"; Platform = "PC"; HasMultiplayer = false }
        { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
        { Developer = "Crystal Dynamics"; Name = "Tomb Raider"; Platform = "PC"; HasMultiplayer = true }
        { Developer = "Crystal Dynamics"; Name = "Tomb Raider"; Platform = "Xbox 360"; HasMultiplayer = true }
        { Developer = "Crystal Dynamics"; Name = "Tomb Raider"; Platform = "PS3"; HasMultiplayer = true }
        { Developer = "Crystal Dynamics"; Name = "Tomb Raider"; Platform = "Xbox One"; HasMultiplayer = true }
        { Developer = "Crystal Dynamics"; Name = "Tomb Raider"; Platform = "PS4"; HasMultiplayer = true }
    ]

    member this.FromGameTable q = fromTable tableClient this.Name q
    member this.FromGameTableAsync q = fromTableAsync tableClient this.Name q
    member this.InsertTestData () = do data |> insertInParallelAndCheckSuccess tableClient this.Name

type SimpleTempTable (tableClient) =
    inherit Storage.TempTable (tableClient)

    member this.InsertTestData () =
        //Storage emulator segments the data after 1000 rows, so generate 1200 rows
        let rows =
                seq {
                    for partition in 1..12 do
                    for row in 1..100 do
                    yield { PK = "PK" + partition.ToString(); RK = "RK" + row.ToString() }
                }
                |> Array.ofSeq
        do rows |> insertInParallelAndCheckSuccess tableClient this.Name
        rows

let tests connectionString =
    let account = CloudStorageAccount.Parse connectionString
    let tableClient = account.CreateCloudTableClient()

    let gameTestCase name testFn =
        testCase name (fun () ->
            use tempTable = new GameTempTable (tableClient)
            do tempTable.InsertTestData()
            do testFn tempTable
        )

    let gameTestCaseAsync name testFn =
        testCaseAsync name (async {
            use tempTable = new GameTempTable (tableClient)
            do tempTable.InsertTestData()
            do! testFn tempTable
        })

    testList "Data Query Tests" [
        gameTestCase "query by specific instance" <| fun ts ->
            let halo4 =
                Query.all<Game>
                |> Query.where <@ fun _ s -> s.PartitionKey = "343 Industries" && s.RowKey = "Halo 4-Xbox 360" @>
                |> ts.FromGameTable
                |> Seq.toArray

            halo4 |> verifyRecords [|
                { Developer = "343 Industries"; Name = "Halo 4"; Platform = "Xbox 360"; HasMultiplayer = true }
            |]

            halo4 |> verifyMetadata

        gameTestCase "query by partition key" <| fun ts ->
            let valveGames =
                Query.all<Game>
                |> Query.where <@ fun _ s -> s.PartitionKey = "Valve" @>
                |> ts.FromGameTable
                |> Seq.toArray

            valveGames |> verifyRecords [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Portal"; Platform = "PC"; HasMultiplayer = false }
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
            |]

            valveGames |> verifyMetadata


        gameTestCase "query by properties" <| fun ts ->
            let valveGames =
                Query.all<Game>
                |> Query.where <@ fun g _ -> (g.Platform = "Xbox 360" || g.Platform = "PC") && not (g.Developer = "Bungie") @>
                |> ts.FromGameTable
                |> Seq.toArray

            valveGames |> verifyRecords [|
                { Developer = "343 Industries"; Name = "Halo 4"; Platform = "Xbox 360"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Portal"; Platform = "PC"; HasMultiplayer = false }
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
                { Developer = "Crystal Dynamics"; Name = "Tomb Raider"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Crystal Dynamics"; Name = "Tomb Raider"; Platform = "Xbox 360"; HasMultiplayer = true }
            |]

            valveGames |> verifyMetadata

        gameTestCase "query with take" <| fun ts ->
            let valveGames =
                Query.all<Game>
                |> Query.where <@ fun _ s -> s.PartitionKey = "Valve" @>
                |> Query.take 2
                |> ts.FromGameTable
                |> Seq.toArray

            valveGames |> verifyRecords [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
            |]

            valveGames |> verifyMetadata

        gameTestCaseAsync "async query" <| fun ts -> async {
            let! valveGamesSeq =
                Query.all<Game>
                |> Query.where <@ fun _ s -> s.PartitionKey = "Valve" @>
                |> ts.FromGameTableAsync

            let valveGames = valveGamesSeq |> Seq.toArray

            valveGames |> verifyRecords [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Portal"; Platform = "PC"; HasMultiplayer = false }
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
            |]

            valveGames |> verifyMetadata
        }

        testCase "segmented query" <| fun () ->
            use ts = new SimpleTempTable (tableClient)
            let rows = ts.InsertTestData()
            let (simples1, segmentToken1) =
                Query.all<Simple>
                |> fromTableSegmented tableClient ts.Name None

            segmentToken1 |> Expect.isSome "SegmentToken1 should be Some"

            let (simples2, segmentToken2) =
                Query.all<Simple>
                |> fromTableSegmented tableClient ts.Name segmentToken1

            segmentToken2 |> Expect.isNone "SegmentToken1 should be None"

            let allSimples = [simples1; simples2] |> Seq.concat |> Seq.toArray
            allSimples |> verifyRecords rows
            allSimples |> verifyMetadata

        testCaseAsync "async segmented query" <| async {
            use ts = new SimpleTempTable (tableClient)
            let rows = ts.InsertTestData()
            let! (simples1, segmentToken1) =
                Query.all<Simple>
                |> fromTableSegmentedAsync tableClient ts.Name None

            segmentToken1 |> Expect.isSome "SegmentToken1 should be Some"

            let! (simples2, segmentToken2) =
                Query.all<Simple>
                |> fromTableSegmentedAsync tableClient ts.Name segmentToken1

            segmentToken2 |> Expect.isNone "SegmentToken1 should be None"

            let allSimples = [simples1; simples2] |> Seq.concat |> Seq.toArray
            allSimples |> verifyRecords rows
            allSimples |> verifyMetadata
        }

        testCaseAsync "async query that crosses segments with a take that is greater than the segment size" <| async {
            use ts = new SimpleTempTable (tableClient)
            do ts.InsertTestData() |> ignore
            let! results =
                Query.all<Simple>
                |> Query.take 1100
                |> fromTableAsync tableClient ts.Name

            results |> Seq.length |> Expect.equal "Should return the number of rows taken" 1100
        }

        gameTestCase "query with a type that has system properties on it" <| fun ts ->
            let valveGames =
                Query.all<TypeWithSystemProps>
                |> Query.where <@ fun _ s -> s.PartitionKey = "Valve" @>
                |> fromTable tableClient ts.Name
                |> Seq.toArray

            valveGames |> Seq.map (fun (g, _) -> g.PartitionKey) |> Expect.allEqual "All PKs should be Valve" "Valve"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Half-Life 2" "Half-Life 2-PC"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Portal" "Portal-PC"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Portal 2" "Portal 2-PC"
            valveGames |> Seq.map (fun (g, _) -> g.Timestamp) |> Seq.filter (fun ts -> ts = (DateTimeOffset())) |> Expect.isEmpty "All timestamps should be non-default"

            valveGames |> verifyMetadata


        gameTestCase "query with a type that has system properties annotations on it" <| fun ts ->
            let valveGames =
                Query.all<TypeWithSystemPropsAttributes>
                |> Query.where <@ fun _ s -> s.PartitionKey = "Valve" @>
                |> fromTable tableClient ts.Name
                |> Seq.toArray

            valveGames |> Seq.map (fun (g, _) -> g.PartitionKey) |> Expect.allEqual "All PKs should be Valve" "Valve"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Half-Life 2" "Half-Life 2-PC"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Portal" "Portal-PC"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Portal 2" "Portal 2-PC"
            valveGames |> Seq.map (fun (g, _) -> g.Modified) |> Seq.choose id |> Seq.filter (fun ts -> ts = (DateTimeOffset())) |> Expect.isEmpty "All timestamps should be non-default"
            valveGames |> Seq.map (fun (g, _) -> g.tag) |> Seq.choose id |> Seq.filter (fun tag -> tag |> String.IsNullOrEmpty) |> Expect.isEmpty "All etags should be non-default"

            valveGames |> verifyMetadata

        gameTestCase "query with a table entity type" <| fun ts ->
            let valveGames =
                Query.all<GameTableEntity>
                |> Query.where <@ fun _ s -> s.PartitionKey = "Valve" @>
                |> fromTable tableClient ts.Name
                |> Seq.toArray

            valveGames |> verifyRecords [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Portal"; Platform = "PC"; HasMultiplayer = false }
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
            |]

            valveGames |> Seq.map (fun (g, _) -> g.PartitionKey) |> Expect.allEqual "All PKs should be Valve" "Valve"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Half-Life 2" "Half-Life 2-PC"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Portal" "Portal-PC"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Portal 2" "Portal 2-PC"
            valveGames |> Seq.map (fun (g, _) -> g.Timestamp) |> Seq.filter (fun ts -> ts = (DateTimeOffset())) |> Expect.isEmpty "All timestamps should be non-default"

            valveGames |> verifyMetadata

        gameTestCase "query with a table entity type that has an ignored property" <| fun ts ->
            let valveGames =
                Query.all<GameTableEntityWithIgnoredProperty>
                |> Query.where <@ fun _ s -> s.PartitionKey = "Valve" @>
                |> fromTable tableClient ts.Name
                |> Seq.toArray

            valveGames |> verifyRecords [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Portal"; Platform = "PC"; HasMultiplayer = false }
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
            |]

            valveGames |> Seq.map (fun (g, _) -> g.PartitionKey) |> Expect.allEqual "All PKs should be Valve" "Valve"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Half-Life 2" "Half-Life 2-PC"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Portal" "Portal-PC"
            valveGames |> Seq.map (fun (g, _) -> g.RowKey) |> Expect.contains "Games should contain Portal 2" "Portal 2-PC"
            valveGames |> Seq.map (fun (g, _) -> g.Timestamp) |> Seq.filter (fun ts -> ts = (DateTimeOffset())) |> Expect.isEmpty "All timestamps should be non-default"

            //Check that all HasMultiplayers are false as they should have not been populated as they are ignored
            valveGames |> Seq.map (fun (g, _) -> g.HasMultiplayer) |> Expect.allEqual "All HasMultiplayers are false" false

            valveGames |> verifyMetadata

        gameTestCase "querying with types that aren't records or implement ITableEntity fails" <| fun ts ->
            (fun () -> Query.all<NonTableEntityClass> |> fromTable tableClient ts.Name |> ignore)
                |> Expect.throwsT<Exception> "Throws exception"

        gameTestCase "querying for a record that has option type fields works" <| fun ts ->
            let game =
                { Name = "Transistor"
                  Platform = "PC"
                  Developer = "Supergiant Games"
                  HasMultiplayer = Some false
                  Notes = None }

            let result = game |> Insert |> inTable tableClient ts.Name
            result.HttpStatusCode |> Expect.equal "Status code should be 204" 204

            let retrievedGame =
                Query.all<GameWithOptions>
                |> Query.where <@ fun _ s -> s.PartitionKey = game.Developer && s.RowKey = game.Name + "-" + game.Platform @>
                |> ts.FromGameTable
                |> Seq.head
                |> fst

            retrievedGame |> Expect.equal "Retrieved game should be correct" game

        gameTestCase "querying for a record that has option type fields works when filtering by the option-types properties" <| fun ts ->
            let game =
                { Name = "Transistor"
                  Platform = "PC"
                  Developer = "Supergiant Games"
                  HasMultiplayer = Some false
                  Notes = Some "From the same studio that made Bastion" }

            let result = game |> Insert |> inTable tableClient ts.Name
            result.HttpStatusCode |> Expect.equal "Status code should be 204" 204

            let retrievedGame =
                Query.all<GameWithOptions>
                |> Query.where <@ fun g _ -> g.HasMultiplayer = Some false && g.Notes = game.Notes @>
                |> ts.FromGameTable
                |> Seq.head
                |> fst

            retrievedGame |> Expect.equal "Retrieved game should be correct" game

        gameTestCase "querying for a record and filtering using a None option value is not supported by table storage" <| fun ts ->
            let game =
                { Name = "Transistor"
                  Platform = "PC"
                  Developer = "Supergiant Games"
                  HasMultiplayer = None
                  Notes = Some "From the same studio that made Bastion" }

            let result = game |> Insert |> inTable tableClient ts.Name
            result.HttpStatusCode |> Expect.equal "Status code should be 204" 204

            (fun () -> Query.all<GameWithOptions>
                       |> Query.where <@ fun g _ -> g.HasMultiplayer = None && g.Notes = game.Notes @>
                       |> ignore)
            |> Expect.throwsT<Exception> "Throws exception"

        gameTestCase "querying for a record type that is internal works" <| fun ts ->
            let game =
                { InternalGame.Name = "Transistor"
                  Developer = "Supergiant Games" }

            let result = game |> Insert |> inTable tableClient ts.Name
            result.HttpStatusCode |> Expect.equal "Status code should be 204" 204

            let retrievedGame =
                Query.all<InternalGame>
                |> Query.where <@ fun _ s -> s.PartitionKey = game.Developer && s.RowKey = game.Name @>
                |> ts.FromGameTable
                |> Seq.head
                |> fst

            retrievedGame |> Expect.equal "Retrieved game should be correct" game

        gameTestCase "querying for a record type that uses a DateTime and DateTimeOffset property work" <| fun ts ->
            let game =
                { Name = "Transistor"
                  Developer = "Supergiant Games"
                  DevelopmentDate = DateTime(2014, 5, 20, 0, 0, 0, DateTimeKind.Utc)
                  DevelopmentDateAsOffset = DateTimeOffset(2014, 5, 20, 0, 0, 0, TimeSpan.Zero) }

            let result = game |> Insert |> inTable tableClient ts.Name
            result.HttpStatusCode |> Expect.equal "Status code should be 204" 204

            let retrievedGame =
                Query.all<GameWithDateTime>
                |> Query.where <@ fun g _ -> g.DevelopmentDate = game.DevelopmentDate && g.DevelopmentDateAsOffset = game.DevelopmentDateAsOffset @>
                |> ts.FromGameTable
                |> Seq.head
                |> fst

            retrievedGame |> Expect.equal "Retrieved game should be correct" game

        gameTestCase "querying for a record type with URI works" <| fun ts ->
            let game =
                { GameWithUri.Name = "Transistor"
                  Developer = "Supergiant Games"
                  HasMultiplayer = true
                  Website = Uri ("https://example.org")}

            let result = game |> Insert |> inTable tableClient ts.Name
            result.HttpStatusCode |> Expect.equal "Status code should be 204" 204

            let retrievedGame =
                Query.all<GameWithUri>
                |> Query.where <@ fun _ s -> s.PartitionKey = game.Developer && s.RowKey = game.Name @>
                |> ts.FromGameTable
                |> Seq.head
                |> fst

            retrievedGame |> Expect.equal "Retrieved game should be correct" game

        gameTestCase "querying for a record type with URI option works" <| fun ts ->
            let game =
                { GameWithUriOptions.Name = "Transistor"
                  Developer = "Supergiant Games"
                  Website = Some (Uri ("https://example.org"))}

            let result = game |> Insert |> inTable tableClient ts.Name
            result.HttpStatusCode |> Expect.equal "Status code should be 204" 204

            let retrievedGame =
                Query.all<GameWithUriOptions>
                |> Query.where <@ fun _ s -> s.PartitionKey = game.Developer && s.RowKey = game.Name @>
                |> ts.FromGameTable
                |> Seq.head
                |> fst

            retrievedGame |> Expect.equal "Retrieved game should be correct" game

        testCase "querying for a record type with union works" <| fun () ->
            use ts = new SimpleTempTable (tableClient)
            let data = { PartitionKey = "PK"; RowKey = "RK"; UnionProp = A }

            data |> Insert |> inTable tableClient ts.Name |> ignore

            let (output, _) =
                Query.all<TypeWithUnionProperty>
                |> fromTable tableClient ts.Name
                |> Seq.head
            output.UnionProp |> Expect.equal "Retrieved union property correctly" A

        testCase "querying for a record type with union with fields fails" <| fun () ->
            use ts = new SimpleTempTable (tableClient)
            let data = { PartitionKey = "PK"; RowKey = "RK"; UnionProp = A }

            data |> Insert |> inTable tableClient ts.Name |> ignore

            (fun () -> 
                Query.all<TypeWithUnionWithFeidlProperty>
                |> fromTable tableClient ts.Name
                |> Seq.head
                |> ignore )
            |> Expect.throws "Union with field"

        testCase "querying for a record type with enum works" <| fun () ->
            use ts = new SimpleTempTable (tableClient)
            let data = { PartitionKey = "PK"; RowKey = "RK"; EnumProp = EnumProperty.C }

            data |> Insert |> inTable tableClient ts.Name |> ignore

            let (output, _) =
                Query.all<TypeWithEnumProperty>
                |> fromTable tableClient ts.Name
                |> Seq.head
            output.EnumProp |> Expect.equal "Retrieved enum property correctly" EnumProperty.C

    ]