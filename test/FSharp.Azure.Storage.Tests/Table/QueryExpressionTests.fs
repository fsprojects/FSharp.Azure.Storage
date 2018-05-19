module FSharp.Azure.Storage.Tests.Table.QueryExpressionTests

open System
open FSharp.Azure.Storage.Table
open Expecto
open Expecto.Flip

type GameWithOptions =
    { Name: string
      Platform: string option
      Developer : string
      HasMultiplayer: bool option }

let gameTypePropertySet = ["Name"; "Platform"; "Developer"; "HasMultiplayer"] |> Set.ofList

type Game =
    { Name: string
      Platform: string
      Developer : string
      HasMultiplayer: bool }

let gameFilterTestCase testName whereExpr expectedFilterExpr =
    testCase testName <| fun () ->
        let query = Query.all<Game> |> Query.where whereExpr

        query.Filter |> Expect.equal "Correct filter expression" expectedFilterExpr
        query.TakeCount.IsNone |> Expect.isTrue "No take count"
        query.SelectColumns |> Expect.equal "All columns should be selected" gameTypePropertySet

let tests =
    testList "Query Expression Tests" [
        testCase "all returns empty query" <| fun () ->
            let query = Query.all<Game>

            query |> Expect.equal "Query should be empty query" (EntityQuery<Game>.get_Zero ())
            query.SelectColumns |> Expect.equal "All columns should be selected" gameTypePropertySet

        gameFilterTestCase "where query with equals against value"
            <@ fun g _ -> g.Name = "Half-Life 2" @>
            "Name eq 'Half-Life 2'"

        gameFilterTestCase "where query with not equals against value"
            <@ fun g _ -> g.Name <> "Half-Life 2" @>
            "Name ne 'Half-Life 2'"

        gameFilterTestCase "where query with greater than against value"
            <@ fun g _ -> g.Name > "Half-Life 2" @>
            "Name gt 'Half-Life 2'"

        gameFilterTestCase "where query with greater than or equals against value"
            <@ fun g _ -> g.Name >= "Half-Life 2" @>
            "Name ge 'Half-Life 2'"

        gameFilterTestCase "where query with less than against value"
            <@ fun g _ -> g.Name < "Half-Life 2" @>
            "Name lt 'Half-Life 2'"

        gameFilterTestCase "where query with less than or equals against value"
            <@ fun g _ -> g.Name <= "Half-Life 2" @>
            "Name le 'Half-Life 2'"

        gameFilterTestCase "where query with logical and operator"
            <@ fun g _ -> g.Name = "Half-Life 2" && g.Developer = "Valve" @>
            "(Name eq 'Half-Life 2') and (Developer eq 'Valve')"

        gameFilterTestCase "where query with logical or operator"
            <@ fun g _ -> g.Name = "Half-Life 2" || g.Developer = "Valve" @>
            "(Name eq 'Half-Life 2') or (Developer eq 'Valve')"

        gameFilterTestCase "where query with logical or and and operators uses correct operator precedence"
            <@ fun g _ -> g.Name = "Half-Life 2" || g.Developer = "Crystal Dynamics" && g.Name = "Tomb Raider" @>
            "(Name eq 'Half-Life 2') or ((Developer eq 'Crystal Dynamics') and (Name eq 'Tomb Raider'))"

        gameFilterTestCase "where query with logical or and and operators and parentheses"
            <@ fun g _ -> (g.Name = "Half-Life 2" || g.Name = "Portal") && g.Developer = "Valve" @>
            "((Name eq 'Half-Life 2') or (Name eq 'Portal')) and (Developer eq 'Valve')"

        gameFilterTestCase "where query with comparison value coming from outside let binding"
            (let gameName = "Half-Life 2" in <@ fun g _ -> g.Name = gameName @>)
            "Name eq 'Half-Life 2'"

        gameFilterTestCase "where query with comparison value coming from outside object"
            (let game = { Name = "Half-Life 2"; Platform = "PC"; Developer = "Valve"; HasMultiplayer = true }
             <@ fun g _ -> g.Name = game.Name @>)
            "Name eq 'Half-Life 2'"

        gameFilterTestCase "where query allows commutative comparison with equals"
            <@ fun g _ -> "Halo" = g.Name @>
            "Name eq 'Halo'"

        gameFilterTestCase "where query allows commutative comparison with not equals"
            <@ fun g _ -> "Halo" <> g.Name @>
            "Name ne 'Halo'"

        gameFilterTestCase "where query allows commutative comparison with greater than"
            <@ fun g _ -> "Halo" > g.Name @>
            "Name lt 'Halo'"

        gameFilterTestCase "where query allows commutative comparison with greater than or equals"
            <@ fun g _ -> "Halo" >= g.Name @>
            "Name le 'Halo'"

        gameFilterTestCase "where query allows commutative comparison with less than"
            <@ fun g _ -> "Halo" < g.Name @>
            "Name gt 'Halo'"

        gameFilterTestCase "where query allows commutative comparison with less than or equals"
            <@ fun g _ -> "Halo" <= g.Name @>
            "Name ge 'Halo'"

        gameFilterTestCase "where query allows bool properties to be treated as comparison against true"
            <@ fun g _ -> g.HasMultiplayer @>
            "HasMultiplayer eq true"

        gameFilterTestCase "where query allows notted bool properties to be treated as comparison against false"
            <@ fun g _ -> not g.HasMultiplayer @>
            "HasMultiplayer eq false"

        gameFilterTestCase "where query allows bool properties to be combined with other comparisons"
            <@ fun g _ -> g.HasMultiplayer || g.Developer = "Valve" @>
            "(HasMultiplayer eq true) or (Developer eq 'Valve')"

        gameFilterTestCase "where query allows not to be used against property comparison"
            <@ fun g _ -> not (g.Developer = "Valve") @>
            "not (Developer eq 'Valve')"

        gameFilterTestCase "where query allows not to be used against boolean expressions"
            <@ fun g _ -> not (g.Developer = "Valve" && g.Name = "Portal") @>
            "not ((Developer eq 'Valve') and (Name eq 'Portal'))"

        testCase "where query allows comparison against option types" <| fun () ->
            let query =
                Query.all<GameWithOptions>
                |> Query.where <@ fun g _ -> g.Platform = Some "Valve" && g.HasMultiplayer = Some true @>

            query.Filter |> Expect.equal "Correct filter expression" "(Platform eq 'Valve') and (HasMultiplayer eq true)"
            query.TakeCount.IsNone |> Expect.isTrue "No take count"
            query.SelectColumns |> Expect.equal "All columns should be selected" gameTypePropertySet

        testCase "where query does not allow comparison against option types with None" <| fun () ->
            (fun () ->
                Query.all<GameWithOptions>
                |> Query.where <@ fun g _ -> (g.Platform = None && g.HasMultiplayer = None) @>
                |> ignore)
                |> Expect.throwsT<Exception> "Throws exception"

        testCase "multiple where queries are anded together" <| fun () ->
            let query =
                Query.all<Game>
                |> Query.where <@ fun g _ -> g.Name = "Halo 4" @>
                |> Query.where <@ fun g _ -> g.Developer = "343 Studios" @>

            query.Filter |> Expect.equal "Correct filter expression" "(Name eq 'Halo 4') and (Developer eq '343 Studios')"
            query.TakeCount.IsNone |> Expect.isTrue "No take count"
            query.SelectColumns |> Expect.equal "All columns should be selected" gameTypePropertySet

        gameFilterTestCase "partition key where query comparison"
            <@ (fun _ s -> s.PartitionKey = "Valve") @>
            "PartitionKey eq 'Valve'"

        gameFilterTestCase "row key where query comparison"
            <@ (fun _ s -> s.RowKey <> "PS4") @>
            "RowKey ne 'PS4'"

        gameFilterTestCase "timestamp where query comparison"
            (let datetime = DateTimeOffset(2014, 4, 1, 12, 0, 0, TimeSpan.FromHours(11.0))
             <@ (fun _ s -> s.Timestamp > datetime) @>)
            "Timestamp gt datetime'2014-04-01T01:00:00.0000000Z'"

        gameFilterTestCase "where query allows system and entity properties to be all used together"
            (let datetime = DateTimeOffset(2014, 4, 1, 12, 0, 0, TimeSpan.FromHours(11.0))
             <@ fun g s ->
                    g.Name >= "Halo" && g.Name < "I" &&
                    s.PartitionKey = "Bungie" && s.RowKey = "Xbox 360" &&
                    s.Timestamp < datetime @>)
            "((((Name ge 'Halo') and (Name lt 'I')) and (PartitionKey eq 'Bungie')) and (RowKey eq 'Xbox 360')) and (Timestamp lt datetime'2014-04-01T01:00:00.0000000Z')"

        testCase "take sets take count on query" <| fun () ->
            let query = Query.all<Game> |> Query.take 10

            query.Filter |> Expect.equal "Empty filter expression" ""
            query.TakeCount |> Expect.equal "Correct take count" (Some 10)
            query.SelectColumns |> Expect.equal "All columns should be selected" gameTypePropertySet

        testCase "multiple takes uses the smallest take count on query" <| fun () ->
            let query = Query.all<Game> |> Query.take 30 |> Query.take 10 |> Query.take 20

            query.Filter |> Expect.equal "Empty filter expression" ""
            query.TakeCount |> Expect.equal "Correct take count" (Some 10)
            query.SelectColumns |> Expect.equal "All columns should be selected" gameTypePropertySet

        testCase "take combines with where query" <| fun () ->
            let query =
                Query.all<Game>
                |> Query.where <@ fun _ s -> s.PartitionKey = "Blizzard" @>
                |> Query.take 5

            query.Filter |> Expect.equal "Correct filter expression" "PartitionKey eq 'Blizzard'"
            query.TakeCount |> Expect.equal "Correct take count" (Some 5)
            query.SelectColumns |> Expect.equal "All columns should be selected" gameTypePropertySet

        testCase "manual modification of query select columns is respected by subsequent query transformations" <| fun () ->
            let properties = [ "Name"; "Platform" ] |> Set.ofList
            let query =
                Query.all<Game>
                |> (fun q -> { q with SelectColumns = properties })
                |> Query.where <@ fun g s -> s.PartitionKey = "Blizzard" @>
                |> Query.take 5

            query.Filter |> Expect.equal "Correct filter expression" "PartitionKey eq 'Blizzard'"
            query.TakeCount |> Expect.equal "Correct take count" (Some 5)
            query.SelectColumns |> Expect.equal "Correct columns selected" properties
    ]