namespace DigitallyCreated.FSharp.Azure.IntegrationTests

open System
open DigitallyCreated.FSharp.Azure.TableStorage
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open Xunit
open FsUnit.Xunit

module QueryExpression = 
    
    type GameWithOptions = 
        { Name: string
          Platform: string option
          Developer : string
          HasMultiplayer: bool option }

    type Game = 
        { Name: string
          Platform: string
          Developer : string
          HasMultiplayer: bool }

    type Tests() =
        
        [<Fact>]
        let ``all returns empty query``() =
            let query = Query.all<Game>

            query |> should equal (EntityQuery<Game>.get_Zero ())

        [<Fact>]
        let ``where query with equals against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.Name = "Half-Life 2" @>

            query.Filter |> should equal "Name eq 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with not equals against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.Name <> "Half-Life 2" @>

            query.Filter |> should equal "Name ne 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with greater than against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.Name > "Half-Life 2" @>

            query.Filter |> should equal "Name gt 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with greater than or equals against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.Name >= "Half-Life 2" @>

            query.Filter |> should equal "Name ge 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with less than against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.Name < "Half-Life 2" @>

            query.Filter |> should equal "Name lt 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with less than or equals against value`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.Name <= "Half-Life 2" @>

            query.Filter |> should equal "Name le 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with logical and operator`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.Name = "Half-Life 2" && g.Developer = "Valve" @>

            query.Filter |> should equal "(Name eq 'Half-Life 2') and (Developer eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with logical or operator`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.Name = "Half-Life 2" || g.Developer = "Valve" @>

            query.Filter |> should equal "(Name eq 'Half-Life 2') or (Developer eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with logical or and and operators uses correct operator precedence`` () = 
            let query = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> g.Name = "Half-Life 2" || g.Developer = "Crystal Dynamics" && g.Name = "Tomb Raider" @>

            query.Filter |> should equal "(Name eq 'Half-Life 2') or ((Developer eq 'Crystal Dynamics') and (Name eq 'Tomb Raider'))"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with logical or and and operators and parentheses`` () = 
            let query = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> (g.Name = "Half-Life 2" || g.Name = "Portal") && g.Developer = "Valve" @>

            query.Filter |> should equal "((Name eq 'Half-Life 2') or (Name eq 'Portal')) and (Developer eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with comparison value coming from outside let binding`` () = 
            let gameName = "Half-Life 2"
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.Name = gameName @>

            query.Filter |> should equal "Name eq 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query with comparison value coming from outside object`` () = 
            let game = { Name = "Half-Life 2"; Platform = "PC"; Developer = "Valve"; HasMultiplayer = true }
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.Name = game.Name @>

            query.Filter |> should equal "Name eq 'Half-Life 2'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with equals`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> "Halo" = g.Name @>

            query.Filter |> should equal "Name eq 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with not equals`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> "Halo" <> g.Name @>

            query.Filter |> should equal "Name ne 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with greater than`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> "Halo" > g.Name @>

            query.Filter |> should equal "Name lt 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with greater than or equals`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> "Halo" >= g.Name @>

            query.Filter |> should equal "Name le 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with less than`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> "Halo" < g.Name @>

            query.Filter |> should equal "Name gt 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows commutative comparison with less than or equals`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> "Halo" <= g.Name @>

            query.Filter |> should equal "Name ge 'Halo'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows bool properties to be treated as comparison against true`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.HasMultiplayer @>

            query.Filter |> should equal "HasMultiplayer eq true"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows notted bool properties to be treated as comparison against false`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> not g.HasMultiplayer @>

            query.Filter |> should equal "HasMultiplayer eq false"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows bool properties to be combined with other comparisons`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> g.HasMultiplayer || g.Developer = "Valve" @>

            query.Filter |> should equal "(HasMultiplayer eq true) or (Developer eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows not to be used against property comparison`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> not (g.Developer = "Valve") @>

            query.Filter |> should equal "not (Developer eq 'Valve')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows not to be used against boolean expressions`` () = 
            let query = Query.all<Game> |> Query.where <@ fun g s -> not (g.Developer = "Valve" && g.Name = "Portal") @>

            query.Filter |> should equal "not ((Developer eq 'Valve') and (Name eq 'Portal'))"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows comparison against option types`` () =
            let query = 
                Query.all<GameWithOptions> 
                |> Query.where <@ fun g s -> g.Platform = Some "Valve" && g.HasMultiplayer = Some true @>

            query.Filter |> should equal "(Platform eq 'Valve') and (HasMultiplayer eq true)"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query does not allow comparison against option types with None`` () =
            (fun () ->
                Query.all<GameWithOptions> 
                |> Query.where <@ fun g s -> (g.Platform = None && g.HasMultiplayer = None) @>
                |> ignore)
                |> should throw typeof<Exception>

        [<Fact>]
        let ``multiple where queries are anded together`` () = 
            let query = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> g.Name = "Halo 4" @>
                |> Query.where <@ fun g s -> g.Developer = "343 Studios" @>

            query.Filter |> should equal "(Name eq 'Halo 4') and (Developer eq '343 Studios')"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``partition key where query comparison`` () = 
            let query = Query.all<Game> |> Query.where <@ (fun g s -> s.PartitionKey = "Valve") @>

            query.Filter |> should equal "PartitionKey eq 'Valve'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``row key where query comparison`` () = 
            let query = Query.all<Game> |> Query.where <@ (fun g s -> s.RowKey <> "PS4") @>

            query.Filter |> should equal "RowKey ne 'PS4'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``timestamp where query comparison`` () = 
            let datetime = DateTimeOffset(2014, 4, 1, 12, 0, 0, TimeSpan.FromHours(11.0))
            let query = Query.all<Game> |> Query.where <@ (fun g s -> s.Timestamp > datetime) @>

            query.Filter |> should equal "Timestamp gt datetime'2014-04-01T01:00:00.0000000Z'"
            query.TakeCount.IsNone |> should equal true

        [<Fact>]
        let ``where query allows system and entity properties to be all used together`` () = 
            let datetime = DateTimeOffset(2014, 4, 1, 12, 0, 0, TimeSpan.FromHours(11.0))

            let query = 
                Query.all<Game> 
                |> Query.where <@ fun g s -> 
                    g.Name >= "Halo" && g.Name < "I" && 
                    s.PartitionKey = "Bungie" && s.RowKey = "Xbox 360" &&
                    s.Timestamp < datetime @>

            query.Filter |> should equal "((((Name ge 'Halo') and (Name lt 'I')) and (PartitionKey eq 'Bungie')) and (RowKey eq 'Xbox 360')) and (Timestamp lt datetime'2014-04-01T01:00:00.0000000Z')"
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
            let query = Query.all<Game> |> Query.where <@ fun g s -> s.PartitionKey = "Blizzard" @> |> Query.take 5

            query.Filter |> should equal "PartitionKey eq 'Blizzard'"
            query.TakeCount.IsSome |> should equal true
            query.TakeCount.Value |> should equal 5