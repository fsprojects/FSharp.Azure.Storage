FSharp.Azure.Storage
====================

FSharp.Azure.Storage is a wrapper over the standard Microsoft [Microsoft.Azure.Cosmos.Table][1]
library that allows you to write idiomatic F# when talking to Azure.

The standard storage API is fine when you're writing C#, however when you're
using F# you want to be able to use immutable record types, use the native F#
async support and generally write in a functional style.

[1]: <https://www.nuget.org/packages/Microsoft.Azure.Cosmos.Table>

NuGet [![NuGet Status](https://img.shields.io/nuget/v/FSharp.Azure.Storage.svg?style=flat)](https://www.nuget.org/packages/FSharp.Azure.Storage/)
-----
`Install-Package FSharp.Azure.Storage`

A Quick Taster
--------------
### Inserting/Updating Table Storage
Imagine we had a record type that we wanted to save into table storage:

```f#
open FSharp.Azure.Storage.Table

type Game =
    { [<PartitionKey>] Developer: string
      [<RowKey>] Name: string
      HasMultiplayer: bool }
```

Now we'll define a helper function `inGameTable` that will allow us to persist these Game records to table storage into an existing table called "Games":

```f#
open Microsoft.Azure.Cosmos.Table

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

[2]: https://github.com/fsprojects/FSharp.Azure.Storage/wiki


Building
--------
Run `build.cmd` or `build.sh` to restore the required dependencies using Paket and then build and run tests using FAKE. You can also build in Visual Studio.

By default, the tests run against the Azure Storage Emulator. However, you can run them against any storage account by setting the `FSHARP_AZURE_STORAGE_CONNECTION_STRING` environment variable to an Azure Storage account connection string before running the tests.

**AppVeyor (Windows)**
[![AppVeyor Build status](https://ci.appveyor.com/api/projects/status/ssbhpme5jromcbmo?svg=true)](https://ci.appveyor.com/project/daniel-chambers/fsharp-azure-storage)

**Travis (Linux)**
[![Travis Build Status](https://travis-ci.org/fsprojects/FSharp.Azure.Storage.svg?branch=master)](https://travis-ci.org/fsprojects/FSharp.Azure.Storage)

## Maintainer(s)

- [@daniel-chambers](https://github.com/daniel-chambers)

The default maintainer account for projects under "fsprojects" is [@fsprojectsgit](https://github.com/fsprojectsgit) - F# Community Project Incubation Space (repo management)
