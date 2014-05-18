FSharp.Azure
============

FSharp.Azure is a wrapper over the standard Microsoft [WindowsAzure.Storage][1]
library that allows you to write idiomatic F# when talking to Azure.

The standard storage API is fine when you're writing C#, however when you're
using F# you want to be able to use immutable record types, use the native F#
async support and generally write in a functional style.

[1]: <https://github.com/Azure/azure-storage-net>

NuGet
-----
`Install-Package FSharp.Azure`

A Quick Taster
--------------
### Inserting/Updating Table Storage
Imagine we had a record type that we wanted to save into table storage:

```f#
open DigitallyCreated.FSharp.Azure.TableStorage

type Game = 
    { [<PartitionKey>] Developer: string
      [<RowKey>] Name: string
      HasMultiplayer: bool }
```

Now we'll define a helper function `inGameTable` that will allow us to persist these Game records to table storage into an existing table called "Games":

```f#
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

let account = CloudStorageAccount.Parse "UseDevelopmentStorage=true;" //Or your connection string here
let tableClient = account.CreateCloudTableClient()

let inGameTable game = inTable tableClient "Games" game
```

Now that the set up ceremony is done, let's insert a new Game into table storage:

```f#
let game = { Developer = "343 Industries"; Name = "Halo 4"; HasMultiplayer = true }

let result = game |> Insert |> inGameTable
```

Let's say we want to modify this game and update it in table storage:

```f#
let modifiedGame = { game with HasMultiplayer = false }

let result2 = (modifiedGame, result.Etag) |> Replace |> inGameTable
```

### Querying Table Storage

First we need to set up a little helper function for querying from the "Games" table:

```f#
let fromGameTable q = fromTable tableClient "Games" q
```

Here's how we'd query for an individual record by PartitionKey and RowKey:

```f#
let halo4, metadata = 
    Query.all<Game>
    |> Query.where <@ fun g s -> s.PartitionKey = "343 Industries" && s.RowKey = "Halo 4" @>
    |> fromGameTable
    |> Seq.head
```

If we wanted to find all multiplayer games made by Valve:

```f#
let multiplayerValveGames = 
    Query.all<Game>
    |> Query.where <@ fun g s -> s.PartitionKey = "Valve" && g.HasMultiplayer @>
    |> fromGameTable
```

### Further Information
For further documentation and examples, please visit the [wiki][2].

[2]: https://github.com/daniel-chambers/FSharp.Azure/wiki


Dependencies
------------
* F# 3.1 (VS2013)
* WindowsAzure.Storage v3.1.0.1 - Azure SDK v2.3+ required if you want to use the Storage Emulator
* FSharp.PowerPack v3.0.0