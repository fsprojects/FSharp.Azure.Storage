namespace DigitallyCreated.FSharp.Azure.IntegrationTests

open System
open DigitallyCreated.FSharp.Azure.TableStorage
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Xunit
open FsUnit.Xunit

module TableStorageTests =

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

    type DataModificationTests() = 
    
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


    type QueryExpressionTests() =
        
        [<Fact>]
        let ``all returns empty query``() =
            let query = Query.all<Game>

            query |> should equal (EntityQuery<Game>.get_Zero ())

        [<Fact>]
        let ``where query with equals against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> g.Name = "Half-Life 2" @>

            query.Filter |> should equal "Name eq 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with not equals against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> g.Name <> "Half-Life 2" @>

            query.Filter |> should equal "Name ne 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with greater than against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> g.Name > "Half-Life 2" @>

            query.Filter |> should equal "Name gt 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with greater than or equals against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> g.Name >= "Half-Life 2" @>

            query.Filter |> should equal "Name ge 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with less than against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> g.Name < "Half-Life 2" @>

            query.Filter |> should equal "Name lt 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with less than or equals against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> g.Name <= "Half-Life 2" @>

            query.Filter |> should equal "Name le 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with logical and operator`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> g.Name = "Half-Life 2" && g.Developer = "Valve" @>

            query.Filter |> should equal "(Name eq 'Half-Life 2') AND (Developer eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with logical or operator`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> g.Name = "Half-Life 2" || g.Developer = "Valve" @>

            query.Filter |> should equal "(Name eq 'Half-Life 2') OR (Developer eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with logical or and and operators uses correct operator precedence`` () = 
            let query = 
                Query.all<Game> 
                |> Query.where <@ fun g -> g.Name = "Half-Life 2" || g.Developer = "Eidos" && g.Name = "Tomb Raider" @>

            query.Filter |> should equal "(Name eq 'Half-Life 2') OR ((Developer eq 'Eidos') AND (Name eq 'Tomb Raider'))"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with logical or and and operators and parentheses`` () = 
            let query = 
                Query.all<Game> 
                |> Query.where <@ fun g -> (g.Name = "Half-Life 2" || g.Name = "Portal") && g.Developer = "Valve" @>

            query.Filter |> should equal "((Name eq 'Half-Life 2') OR (Name eq 'Portal')) AND (Developer eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with comparison value coming from outside let binding`` () = 
            let gameName = "Half-Life 2"
            let query = Query.all<Game> |> Query.where <@ fun g -> g.Name = gameName @>

            query.Filter |> should equal "Name eq 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with comparison value coming from outside object`` () = 
            let game = { Name = "Half-Life 2"; Platform = "PC"; Developer = "Valve"; HasMultiplayer = true }
            let query = Query.all<Game> |> Query.where <@ fun g -> g.Name = game.Name @>

            query.Filter |> should equal "Name eq 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with equals`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> "Halo" = g.Name @>

            query.Filter |> should equal "Name eq 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with not equals`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> "Halo" <> g.Name @>

            query.Filter |> should equal "Name ne 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with greater than`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> "Halo" > g.Name @>

            query.Filter |> should equal "Name lt 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with greater than or equals`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> "Halo" >= g.Name @>

            query.Filter |> should equal "Name le 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with less than`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> "Halo" < g.Name @>

            query.Filter |> should equal "Name gt 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with less than or equals`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> "Halo" <= g.Name @>

            query.Filter |> should equal "Name ge 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows bool properties to be treated as comparison against true`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> g.HasMultiplayer @>

            query.Filter |> should equal "HasMultiplayer eq true"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows notted bool properties to be treated as comparison against false`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> not g.HasMultiplayer @>

            query.Filter |> should equal "HasMultiplayer eq false"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows bool properties to be combined with other comparisons`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> g.HasMultiplayer || g.Developer = "Valve" @>

            query.Filter |> should equal "(HasMultiplayer eq true) OR (Developer eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows not to be used against property comparison`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> not (g.Developer = "Valve") @>

            query.Filter |> should equal "NOT (Developer eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows not to be used against boolean expressions`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g -> not (g.Developer = "Valve" && g.Name = "Portal") @>

            query.Filter |> should equal "NOT ((Developer eq 'Valve') AND (Name eq 'Portal'))"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``multiple where queries are anded together`` () = 
            let query = 
                Query.all<Game> 
                |> Query.where <@ fun g -> g.Name = "Halo 4" @>
                |> Query.where <@ fun g -> g.Developer = "343 Studios" @>

            query.Filter |> should equal "(Name eq 'Halo 4') AND (Developer eq '343 Studios')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``partition key where query comparison`` () = 
            let query = Query.all<Game> |> Query.wherePk <@ (fun pk -> pk = "Valve") @>

            query.Filter |> should equal "PartitionKey eq 'Valve'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``partition key where queries are commutative`` () = 
            let query = Query.all<Game> |> Query.wherePk <@ (fun pk -> "Valve" = pk) @>

            query.Filter |> should equal "PartitionKey eq 'Valve'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``partition key where query notted comparison`` () = 
            let query = Query.all<Game> |> Query.wherePk <@ (fun pk -> not (pk = "Valve")) @>

            query.Filter |> should equal "NOT (PartitionKey eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``row key where query comparison`` () = 
            let query = Query.all<Game> |> Query.whereRk <@ (fun rk -> rk <> "PS4") @>

            query.Filter |> should equal "RowKey ne 'PS4'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``row key where queries are commutative`` () = 
            let query = Query.all<Game> |> Query.whereRk <@ (fun rk -> "PS4" <> rk) @>

            query.Filter |> should equal "RowKey ne 'PS4'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``row key where query notted comparison`` () = 
            let query = Query.all<Game> |> Query.whereRk <@ (fun rk -> not (rk = "PS4")) @>

            query.Filter |> should equal "NOT (RowKey eq 'PS4')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``timestamp where query comparison`` () = 
            let datetime = DateTimeOffset(2014, 4, 1, 12, 0, 0, TimeSpan.FromHours(11.0))
            let query = Query.all<Game> |> Query.whereTimestamp <@ (fun t -> t > datetime) @>

            query.Filter |> should equal "Timestamp gt datetime'2014-04-01T01:00:00.0000000Z'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``timestamp where queries are commutative`` () = 
            let datetime = DateTimeOffset(2014, 4, 1, 12, 0, 0, TimeSpan.FromHours(11.0))
            let query = Query.all<Game> |> Query.whereTimestamp <@ (fun t -> datetime < t) @>

            query.Filter |> should equal "Timestamp gt datetime'2014-04-01T01:00:00.0000000Z'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``timestamp where query notted comparison`` () = 
            let datetime = DateTimeOffset(2014, 4, 1, 12, 0, 0, TimeSpan.FromHours(11.0))
            let query = Query.all<Game> |> Query.whereTimestamp <@ (fun t -> not (t > datetime)) @>

            query.Filter |> should equal "NOT (Timestamp gt datetime'2014-04-01T01:00:00.0000000Z')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``all where query types are anded together`` () = 
            let datetime = DateTimeOffset(2014, 4, 1, 12, 0, 0, TimeSpan.FromHours(11.0))

            let query = 
                Query.all<Game> 
                |> Query.wherePk <@ fun pk -> pk = "Bungie" @>
                |> Query.whereRk <@ fun pk -> pk = "Xbox 360" @>
                |> Query.where <@ fun g -> g.Name >= "Halo" && g.Name < "I" @>
                |> Query.whereTimestamp <@ fun t -> t < datetime @>
                

            query.Filter |> should equal "(((PartitionKey eq 'Bungie') AND (RowKey eq 'Xbox 360')) AND ((Name ge 'Halo') AND (Name lt 'I'))) AND (Timestamp lt datetime'2014-04-01T01:00:00.0000000Z')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``take sets take count on query`` () = 
            let query = Query.all<Game> |> Query.take 10

            query.Filter |> should equal ""
            query.TakeCount.IsSome |> should equal true
            query.TakeCount.Value |> should equal 10

        [<Fact>]
        let ``multiple takes uses the smallest take count on query`` () = 
            let query = Query.all<Game> |> Query.take 10 |> Query.take 20

            query.Filter |> should equal ""
            query.TakeCount.IsSome |> should equal true
            query.TakeCount.Value |> should equal 10

        [<Fact>]
        let ``take combines with where query`` () = 
            let query = Query.all<Game> |> Query.wherePk <@ fun pk -> pk = "Blizzard" @> |> Query.take 5

            query.Filter |> should equal "PartitionKey eq 'Blizzard'"
            query.TakeCount.IsSome |> should equal true
            query.TakeCount.Value |> should equal 5
