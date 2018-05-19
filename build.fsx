#r "paket: groupref FakeBuild //"
#load "./.fake/build.fsx/intellisense.fsx"

open System
open Fake.Core
open Fake.DotNet
open Fake.IO
open Fake.Tools
open Fake.Api

#nowarn "52"

let gitHubToken = Environment.environVarOrDefault "GITHUB_TOKEN" ""
let nugetApiKey = Environment.environVarOrDefault "NUGET_KEY" ""

let gitHubOwner = "fsprojects"
let gitHubName = "FSharp.Azure.Storage"

let releaseNotes =
    File.read "./RELEASE_NOTES.md"
    |> ReleaseNotes.parseAll
let latestReleaseNotes = List.head releaseNotes
let previousReleaseNotes = List.item 1 releaseNotes

Target.Description "Tags the current commit with the version and pushes the tag"
Target.create "GitTagAndPush" <| fun _ ->
    if not <| Git.Information.isCleanWorkingCopy "." then
        failwith "Please ensure the working copy is clean before performing a release"

    let remoteUrl = sprintf "github.com/%s/%s" gitHubOwner gitHubName
    let remote =
        Git.CommandHelper.getGitResult "" "remote -v"
        |> Seq.filter (fun (s: string) -> s.EndsWith "(push)")
        |> Seq.tryFind (fun (s: string) -> s.Contains remoteUrl)
        |> function
            | Some (s: string) -> s.Split().[0]
            | None -> failwithf "Unable to determine remote for %s" remoteUrl

    let tag = sprintf "v%s" latestReleaseNotes.NugetVersion
    Git.Branches.pushBranch "." remote <| Git.Information.getBranchName "."
    Git.Branches.tag "." tag
    Git.Branches.pushTag "." remote tag

Target.Description "Generates an AssemblyInfo file with version info"
Target.create "GenerateAssemblyInfoFile" <| fun _ ->
    AssemblyInfoFile.createFSharp "./src/FSharp.Azure.Storage/obj/AssemblyInfo.Generated.fs" [
        AssemblyInfo.Title "FSharp.Azure.Storage"
        AssemblyInfo.Product "FSharp.Azure.Storage"
        AssemblyInfo.Company "Daniel Chambers & Contributors"
        AssemblyInfo.Copyright ("Copyright \169 Daniel Chambers & Contributors " + DateTime.Now.Year.ToString ())
        AssemblyInfo.Version latestReleaseNotes.AssemblyVersion
        AssemblyInfo.FileVersion latestReleaseNotes.AssemblyVersion
        AssemblyInfo.Metadata ("githash", Git.Information.getCurrentHash())
    ]

Target.Description "Compiles the project using dotnet build"
Target.create "Build" <| fun _ ->
    DotNet.build id "./FSharp.Azure.Storage.sln"

Target.Description "Runs the expecto unit tests"
Target.create "Test" <| fun _ ->
    let result = DotNet.exec id "run" "--project ./test/FSharp.Azure.Storage.Tests -c Release"
    if not result.OK then failwithf "Tests failed with code %i" result.ExitCode

Target.Description "Deletes the contents of the ./bin directory"
Target.create "PaketClean" <| fun _ ->
    Shell.cleanDir "./bin"

Target.Description "Creates the NuGet package"
Target.create "PaketPack" <| fun _ ->
    Paket.pack <| fun p ->
        { p with
            ReleaseNotes = latestReleaseNotes.Notes |> List.map (fun s -> "- " + s) |> String.concat "\n"
            Version = latestReleaseNotes.NugetVersion
            OutputPath = "./bin" }

Target.Description "Ensures you have specified your NuGet API key in the NUGET_KEY env var"
Target.create "ValidateNugetApiKey" <| fun _ ->
    if String.IsNullOrWhiteSpace nugetApiKey then
        failwith "Please set the NUGET_KEY environment variable to your NuGet API Key"

Target.Description "Pushes the NuGet package to the package repository"
Target.create "PaketPush" <| fun _ ->
    Paket.push <| fun p ->
        { p with
            WorkingDir = "./bin"
            ApiKey = nugetApiKey }

Target.Description "Ensures you have specified your GitHub personal access token in the GITHUB_TOKEN env var"
Target.create "ValidateGitHubCredentials" <| fun _ ->
        if String.IsNullOrWhiteSpace gitHubToken then
            failwith "Please set the GITHUB_TOKEN environment variable to a GitHub personal access token with repo access."

Target.Description "Creates a release on GitHub with the release notes"
Target.create "GitHubRelease" <| fun _ ->
    let gitHubReleaseNotes =
        [ yield "## Changelog"
          yield! latestReleaseNotes.Notes |> List.map (fun s -> "- " + s)
          yield ""
          yield sprintf "Full changelog [here](https://github.com/fsprojects/FSharp.Azure.Storage/compare/v%s...v%s)" previousReleaseNotes.NugetVersion latestReleaseNotes.NugetVersion ]

    GitHub.createClientWithToken gitHubToken
    |> GitHub.draftNewRelease gitHubOwner gitHubName ("v" + latestReleaseNotes.NugetVersion) (latestReleaseNotes.SemVer.PreRelease <> None) gitHubReleaseNotes
    |> GitHub.publishDraft
    |> Async.RunSynchronously

Target.create "BeginRelease" Target.DoNothing

Target.create "PublishRelease" Target.DoNothing

open Fake.Core.TargetOperators

"GitTagAndPush"
?=> "GenerateAssemblyInfoFile"
==> "Build"
==> "Test"
==> "PaketClean"
==> "PaketPack"
==> "BeginRelease"
==> "PaketPush"
==> "GitHubRelease"
==> "PublishRelease"

"ValidateGitHubCredentials"
?=> "BeginRelease"

"ValidateGitHubCredentials"
==> "GitHubRelease"

"ValidateNugetApiKey"
?=> "BeginRelease"

"ValidateNugetApiKey"
==> "PaketPush"

// Only do a GitTagAndPush if we're pushing a new version
"GitTagAndPush"
==> "PaketPush"

Target.runOrDefault "PaketPack"