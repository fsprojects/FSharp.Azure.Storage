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

    type Tests() = 
        let account = CloudStorageAccount.Parse "UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://ipv4.fiddler"
        let tableClient = account.CreateCloudTableClient()
        let gameTableName = "TestsGame"
        let gameTable = tableClient.GetTableReference gameTableName

        do gameTable.DeleteIfExists() |> ignore
        do gameTable.Create() |> ignore

        let inTableAsync = inTableAsync tableClient
        let fromTable = fromTable tableClient
        let fromTableSegmented = fromTableSegmented tableClient
        let fromTableSegmentedAsync = fromTableSegmentedAsync tableClient
        let fromTableAsync = fromTableAsync tableClient

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

        do data |> Seq.map (fun r -> r |> insert |> inTableAsync gameTableName) 
            |> Async.Parallel |> Async.RunSynchronously |> ignore

        let verifyMetadata metadata = 
            metadata |> Seq.iter (fun (_, m) ->
                m.Timestamp |> should not' (equal (DateTimeOffset()))
                m.Etag |> should not' (be NullOrEmptyString)
            )

        let verifyGames expected actual = 
            actual |> Array.length |> should equal (expected |> Array.length)
            let actual = actual |> Seq.map fst
            expected |> Seq.iter (fun e -> actual |> should contain e)

        [<Fact>]
        let ``query by specific instance``() = 
            let halo4 = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> s.PartitionKey = "343 Studios" && s.RowKey = "Halo 4-Xbox 360" @> 
                |> fromTable gameTableName
                |> Seq.toArray
            
            halo4 |> verifyGames [|
                { Developer = "343 Studios"; Name = "Halo 4"; Platform = "Xbox 360"; HasMultiplayer = true }
            |]

            halo4 |> verifyMetadata

        [<Fact>]
        let ``query by partition key``() =
            let valveGames = 
                Query.all<Game> |> Query.where <@ fun g s -> s.PartitionKey = "Valve" @> |> fromTable gameTableName |> Seq.toArray
            
            valveGames |> verifyGames [|
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
                |> fromTable gameTableName 
                |> Seq.toArray
            
            valveGames |> verifyGames [|
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
                |> fromTable gameTableName 
                |> Seq.toArray
            
            valveGames |> verifyGames [|
                { Developer = "Valve"; Name = "Half-Life 2"; Platform = "PC"; HasMultiplayer = true } 
                { Developer = "Valve"; Name = "Portal 2"; Platform = "PC"; HasMultiplayer = false } 
            |]

            valveGames |> verifyMetadata
