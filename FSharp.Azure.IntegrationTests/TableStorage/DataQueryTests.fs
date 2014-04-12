namespace DigitallyCreated.FSharp.Azure.IntegrationTests

open System
open DigitallyCreated.FSharp.Azure.TableStorage
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Xunit
open FsUnit.Xunit

module DataQuery =

    type Game = 
        { Name: string
          Platform: string
          Developer : string
          HasMultiplayer: bool }
          
          interface ITableIdentifiable with
            member g.GetIdentifier() = 
                { PartitionKey = g.Developer; RowKey = g.Name + "-" + g.Platform }

    type TypeWithSystemProps = 
        { [<PartitionKey>] PartitionKey : string; 
          [<RowKey>] RowKey : string; 
          Timestamp : DateTimeOffset }

    type Simple = { [<PartitionKey>] PK : string; [<RowKey>] RK : string }

    type Tests() = 
        let account = CloudStorageAccount.Parse "UseDevelopmentStorage=true;"
        let tableClient = account.CreateCloudTableClient()
        let gameTableName = "TestsGame"
        let gameTable = tableClient.GetTableReference gameTableName

        do gameTable.DeleteIfExists() |> ignore
        do gameTable.Create() |> ignore

        let inTableAsync = inTableAsync tableClient
        let fromGameTable = fromTable tableClient gameTableName
        let fromGameTableAsync = fromTableAsync tableClient gameTableName

        static let data = [
            { Developer = "343 Studios"; Name = "Halo 4"; Platform = "Xbox 360"; HasMultiplayer = true }
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

        let insertInParallel tableName items = 
            items 
            |> Seq.map (fun r -> r |> insert |> inTableAsync tableName) 
            |> Async.ParallelByDegree 4 
            |> Async.RunSynchronously
            |> Seq.iter (fun r -> r.HttpStatusCode |> should equal 204)

        do data |> insertInParallel gameTableName |> ignore

        let verifyMetadata metadata = 
            metadata |> Seq.iter (fun (_, m) ->
                m.Etag |> should not' (be NullOrEmptyString)
                m.Timestamp |> should not' (equal (DateTimeOffset()))
            )

        let verifyRecords expected actual = 
            actual |> Array.length |> should equal (expected |> Array.length)
            let actual = actual |> Seq.map fst
            expected |> Seq.iter (fun e -> actual |> should contain e)



        [<Fact>]
        let ``query by specific instance``() = 
            let halo4 = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> s.PartitionKey = "343 Studios" && s.RowKey = "Halo 4-Xbox 360" @> 
                |> fromGameTable
                |> Seq.toArray
            
            halo4 |> verifyRecords [|
                { Developer = "343 Studios"; Name = "Halo 4"; Platform = "Xbox 360"; HasMultiplayer = true }
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
                { Developer = "343 Studios"; Name = "Halo 4"; Platform = "Xbox 360"; HasMultiplayer = true }
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

        let simpleTableName = "TestsSimple"
        let fromSimpleTableSegmented = fromTableSegmented tableClient simpleTableName
        let fromSimpleTableSegmentedAsync = fromTableSegmentedAsync tableClient simpleTableName
        let createDataForSegmentQueries() = 
            let simpleTable = tableClient.GetTableReference simpleTableName

            do simpleTable.DeleteIfExists() |> ignore
            do simpleTable.Create() |> ignore

            //Storage emulator segments the data after 1000 rows, so generate 1200 rows
            let rows =
                seq {
                    for partition in 1..12 do
                    for row in 1..100 do
                    yield { PK = "PK" + partition.ToString(); RK = "RK" + row.ToString() }
                }
                |> Array.ofSeq
            do rows |> insertInParallel simpleTableName
            rows

        [<Fact>]
        let ``segmented query``() =
            let rows = createDataForSegmentQueries();

            let (simples1, segmentToken1) = 
                Query.all<Simple> 
                |> fromSimpleTableSegmented None

            segmentToken1.IsSome |> should equal true

            let (simples2, segmentToken2) = 
                Query.all<Simple> 
                |> fromSimpleTableSegmented segmentToken1

            segmentToken2.IsNone |> should equal true

            let allSimples = [simples1; simples2] |> Seq.concat |> Seq.toArray
            allSimples |> verifyRecords rows
            allSimples |> verifyMetadata

        [<Fact>]
        let ``async segmented query``() =
            let rows = createDataForSegmentQueries();

            let (simples1, segmentToken1) = 
                Query.all<Simple> 
                |> fromSimpleTableSegmentedAsync None
                |> Async.RunSynchronously

            segmentToken1.IsSome |> should equal true

            let (simples2, segmentToken2) = 
                Query.all<Simple> 
                |> fromSimpleTableSegmentedAsync segmentToken1
                |> Async.RunSynchronously

            segmentToken2.IsNone |> should equal true

            let allSimples = [simples1; simples2] |> Seq.concat |> Seq.toArray
            allSimples |> verifyRecords rows
            allSimples |> verifyMetadata

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