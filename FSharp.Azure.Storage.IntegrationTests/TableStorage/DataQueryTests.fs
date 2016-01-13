namespace FSharp.Azure.Storage.IntegrationTests

open System
open FSharp.Azure.Storage.Table
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Xunit
open FsUnit.Xunit

module DataQuery =

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

    [<AbstractClass>]
    type DataQueryTests(connectionString : string) = 
        let account = CloudStorageAccount.Parse connectionString
        let tableClient = account.CreateCloudTableClient()
        let gameTableName = Storage.getTableName()
        let gameTable = tableClient.GetTableReference gameTableName

        do Storage.clearTable gameTable

        let fromGameTable q = fromTable tableClient gameTableName q
        let fromGameTableAsync q = fromTableAsync tableClient gameTableName q

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

        let processInParallel tableName operation = 
            Seq.map operation
            >> autobatch
            >> Seq.map (inTableAsBatchAsync tableClient tableName)
            >> Async.ParallelByDegree 4 
            >> Async.RunSynchronously
            >> Seq.concat

        let insertInParallelAndCheckSuccess tableName =
            processInParallel tableName Insert
            >> Seq.iter (fun r -> r.HttpStatusCode |> should equal 204)
        
        do data |> insertInParallelAndCheckSuccess gameTableName

        let verifyMetadata metadata = 
            metadata |> Seq.iter (fun (_, m) ->
                m.Etag |> should not' (be NullOrEmptyString)
                m.Timestamp |> should not' (equal (DateTimeOffset()))
            )

        let verifyRecords expected actual = 
            actual |> Array.length |> should equal (expected |> Array.length)
            let actual = actual |> Seq.map fst
            actual |> Seq.iter (fun a -> expected |> Seq.exists (fun e -> a.Equals(e)) |> should equal true)



        [<Fact>]
        let ``query by specific instance``() = 
            let halo4 = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> s.PartitionKey = "343 Industries" && s.RowKey = "Halo 4-Xbox 360" @> 
                |> fromGameTable
                |> Seq.toArray
            
            halo4 |> verifyRecords [|
                { Developer = "343 Industries"; Name = "Halo 4"; Platform = "Xbox 360"; HasMultiplayer = true }
            |]

            halo4 |> verifyMetadata

        [<Fact>]
        let ``query by partition key``() =
            let valveGames = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> s.PartitionKey = "Valve" @> 
                |> fromGameTable 
                |> Seq.toArray
            
            valveGames |> verifyRecords [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Portal"; Platform = "PC"; HasMultiplayer = false } 
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
            |]

            valveGames |> verifyMetadata

        [<Fact>]
        let ``query by properties``() =
            let valveGames = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> (g.Platform = "Xbox 360" || g.Platform = "PC") && not (g.Developer = "Bungie") @> 
                |> fromGameTable 
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

        [<Fact>]
        let ``query with take``() =
            let valveGames = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> s.PartitionKey = "Valve" @> 
                |> Query.take 2
                |> fromGameTable 
                |> Seq.toArray
            
            valveGames |> verifyRecords [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true } 
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false } 
            |]

            valveGames |> verifyMetadata

        [<Fact>]
        let ``async query``() =
            let valveGames = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> s.PartitionKey = "Valve" @> 
                |> fromGameTableAsync 
                |> Async.RunSynchronously
                |> Seq.toArray
            
            valveGames |> verifyRecords [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Portal"; Platform = "PC"; HasMultiplayer = false } 
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
            |]

            valveGames |> verifyMetadata

        let createDataForSegmentQueries() =
            let simpleTableName = Storage.getTableName()
            let simpleTable = tableClient.GetTableReference simpleTableName
            do Storage.clearTable simpleTable

            //Storage emulator segments the data after 1000 rows, so generate 1200 rows
            let rows =
                seq {
                    for partition in 1..12 do
                    for row in 1..100 do
                    yield { PK = "PK" + partition.ToString(); RK = "RK" + row.ToString() }
                }
                |> Array.ofSeq
            do rows |> insertInParallelAndCheckSuccess simpleTableName
            simpleTable, rows

        [<Fact>]
        let ``segmented query``() =
            let table, rows = createDataForSegmentQueries();
            try
                let (simples1, segmentToken1) = 
                    Query.all<Simple> 
                    |> fromTableSegmented tableClient table.Name None

                segmentToken1.IsSome |> should equal true

                let (simples2, segmentToken2) = 
                    Query.all<Simple> 
                    |> fromTableSegmented tableClient table.Name segmentToken1

                segmentToken2.IsNone |> should equal true

                let allSimples = [simples1; simples2] |> Seq.concat |> Seq.toArray
                allSimples |> verifyRecords rows
                allSimples |> verifyMetadata

            finally table.Delete()

        [<Fact>]
        let ``async segmented query``() =
            let table, rows = createDataForSegmentQueries();
            try
                let (simples1, segmentToken1) = 
                    Query.all<Simple> 
                    |> fromTableSegmentedAsync tableClient table.Name None
                    |> Async.RunSynchronously

                segmentToken1.IsSome |> should equal true

                let (simples2, segmentToken2) = 
                    Query.all<Simple> 
                    |> fromTableSegmentedAsync tableClient table.Name segmentToken1
                    |> Async.RunSynchronously

                segmentToken2.IsNone |> should equal true

                let allSimples = [simples1; simples2] |> Seq.concat |> Seq.toArray
                allSimples |> verifyRecords rows
                allSimples |> verifyMetadata

            finally table.Delete()

        [<Fact>]
        let ``query with a type that has system properties on it``() =

            let valveGames = 
                Query.all<TypeWithSystemProps> 
                |> Query.where <@ fun g s -> s.PartitionKey = "Valve" @> 
                |> fromTable tableClient gameTableName
                |> Seq.toArray
            
            valveGames |> Array.iter (fun (g, _) -> g.PartitionKey |> should equal "Valve")
            valveGames |> Array.exists (fun (g, _) -> g.RowKey = "Half-Life 2-PC" ) |> should equal true
            valveGames |> Array.exists (fun (g, _) -> g.RowKey = "Portal-PC" ) |> should equal true
            valveGames |> Array.exists (fun (g, _) -> g.RowKey = "Portal 2-PC" ) |> should equal true
            valveGames |> Array.iter (fun (g, _) -> g.Timestamp |> should not' (equal (DateTimeOffset())))

            valveGames |> verifyMetadata

        [<Fact>]
        let ``query with a table entity type``() =
            let valveGames = 
                Query.all<GameTableEntity> 
                |> Query.where <@ fun g s -> s.PartitionKey = "Valve" @> 
                |> fromTable tableClient gameTableName
                |> Seq.toArray
            
            valveGames |> verifyRecords [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Portal"; Platform = "PC"; HasMultiplayer = false } 
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
            |]

            valveGames |> Array.iter (fun (g, _) -> g.PartitionKey |> should equal "Valve")
            valveGames |> Array.exists (fun (g, _) -> g.RowKey = "Half-Life 2-PC" ) |> should equal true
            valveGames |> Array.exists (fun (g, _) -> g.RowKey = "Portal-PC" ) |> should equal true
            valveGames |> Array.exists (fun (g, _) -> g.RowKey = "Portal 2-PC" ) |> should equal true
            valveGames |> Array.iter (fun (g, _) -> g.Timestamp |> should not' (equal (DateTimeOffset())))

            valveGames |> verifyMetadata

        [<Fact>]
        let ``query with a table entity type that has an ignored property``() =
            let valveGames = 
                Query.all<GameTableEntityWithIgnoredProperty> 
                |> Query.where <@ fun g s -> s.PartitionKey = "Valve" @> 
                |> fromTable tableClient gameTableName
                |> Seq.toArray
            
            valveGames |> verifyRecords [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true }
                { Developer = "Valve"; Name = "Portal"; Platform = "PC"; HasMultiplayer = false } 
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false }
            |]

            valveGames |> Array.iter (fun (g, _) -> g.PartitionKey |> should equal "Valve")
            valveGames |> Array.exists (fun (g, _) -> g.RowKey = "Half-Life 2-PC" ) |> should equal true
            valveGames |> Array.exists (fun (g, _) -> g.RowKey = "Portal-PC" ) |> should equal true
            valveGames |> Array.exists (fun (g, _) -> g.RowKey = "Portal 2-PC" ) |> should equal true
            valveGames |> Array.iter (fun (g, _) -> g.Timestamp |> should not' (equal (DateTimeOffset())))

            //Check that all HasMultiplayers are false as they should have not been populated as they are ignored
            valveGames |> Array.iter (fun (g, _) -> g.HasMultiplayer |> should equal false)

            valveGames |> verifyMetadata

        [<Fact>]
        let ``querying with types that aren't records or implement ITableEntity fails``() = 
            (fun () -> Query.all<NonTableEntityClass> |> fromTable tableClient gameTableName |> ignore)
                |> should throw typeof<Exception>

        [<Fact>]
        let ``querying for a record that has option type fields works``() = 
            let game = 
                { Name = "Transistor"
                  Platform = "PC"
                  Developer = "Supergiant Games"
                  HasMultiplayer = Some false 
                  Notes = None }

            let result = game |> Insert |> inTable tableClient gameTableName
            result.HttpStatusCode |> should equal 204

            let retrievedGame = 
                Query.all<GameWithOptions> 
                |> Query.where <@ fun g s -> s.PartitionKey = game.Developer && s.RowKey = game.Name + "-" + game.Platform @>
                |> fromGameTable
                |> Seq.head 
                |> fst

            game = retrievedGame |> should equal true

        
        [<Fact>]
        let ``querying for a record that has option type fields works when filtering by the option-types properties``() = 
            let game = 
                { Name = "Transistor"
                  Platform = "PC"
                  Developer = "Supergiant Games"
                  HasMultiplayer = Some false 
                  Notes = Some "From the same studio that made Bastion" }

            let result = game |> Insert |> inTable tableClient gameTableName
            result.HttpStatusCode |> should equal 204

            let retrievedGame = 
                Query.all<GameWithOptions> 
                |> Query.where <@ fun g s -> g.HasMultiplayer = Some false && g.Notes = game.Notes @>
                |> fromGameTable
                |> Seq.head 
                |> fst

            game = retrievedGame |> should equal true

        [<Fact>]
        let ``querying for a record type that is internal works``() = 
            let game = 
                { InternalGame.Name = "Transistor"
                  Developer = "Supergiant Games" }

            let result = game |> Insert |> inTable tableClient gameTableName
            result.HttpStatusCode |> should equal 204

            let retrievedGame = 
                Query.all<InternalGame> 
                |> Query.where <@ fun g s -> s.PartitionKey = game.Developer && s.RowKey = game.Name @>
                |> fromGameTable
                |> Seq.head 
                |> fst

            game = retrievedGame |> should equal true

        [<Fact>]
        let ``querying for a record type that uses a DateTime and DateTimeOffset property works``() = 
            let game = 
                { Name = "Transistor"
                  Developer = "Supergiant Games"
                  DevelopmentDate = DateTime(2014, 5, 20, 0, 0, 0, DateTimeKind.Utc)
                  DevelopmentDateAsOffset = DateTimeOffset(2014, 5, 20, 0, 0, 0, TimeSpan.Zero) }

            let result = game |> Insert |> inTable tableClient gameTableName
            result.HttpStatusCode |> should equal 204

            let retrievedGame = 
                Query.all<GameWithDateTime> 
                |> Query.where <@ fun g s -> g.DevelopmentDate = game.DevelopmentDate && g.DevelopmentDateAsOffset = game.DevelopmentDateAsOffset @>
                |> fromGameTable
                |> Seq.head 
                |> fst

            game = retrievedGame |> should equal true

        interface IDisposable with
            member __.Dispose() = gameTable.Delete()


    [<Trait("Category", "Remote")>]
    type ``Data Query Tests`` () =
        inherit DataQueryTests(ConnectionStrings.fromEnvironment())

    [<Trait("Category", "Emulator")>]
    type ``Data Query Tests (Storage Emulator)`` () =
        inherit DataQueryTests(ConnectionStrings.storageEmulator)