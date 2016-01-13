// --------------------------------------------------------------------------------------
// FAKE build script 
// --------------------------------------------------------------------------------------

#I "packages/FAKE/tools"
#r "packages/FAKE/tools/FakeLib.dll"

open System
open System.IO

open Fake 
open Fake.Git
open Fake.ReleaseNotesHelper
open Fake.AssemblyInfoFile
open Fake.Testing

// --------------------------------------------------------------------------------------
// Information about the project to be used at NuGet and in AssemblyInfo files
// --------------------------------------------------------------------------------------

let project = "FSharp.Azure.Storage"

let gitOwner = "fsprojects"
let gitHome = "https://github.com/" + gitOwner
let gitName = "FSharp.Azure.Storage"
let gitRaw = environVarOrDefault "gitRaw" "https://raw.github.com/" + gitOwner

let remoteTests = environVarOrDefault "RunRemoteTests" "false" |> Boolean.Parse
let emulatorTests = environVarOrDefault "RunEmulatorTests" "false" |> Boolean.Parse

//
//// --------------------------------------------------------------------------------------
//// The rest of the code is standard F# build script 
//// --------------------------------------------------------------------------------------

//// Read release notes & version info from RELEASE_NOTES.md
Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
let release = parseReleaseNotes (IO.File.ReadAllLines "RELEASE_NOTES.md")
let nugetVersion = release.NugetVersion

let testAssemblies = [ "**/bin/Release/FSharp.Azure.Storage.IntegrationTests.dll" ]

Target "BuildVersion" (fun _ ->
    Shell.Exec("appveyor", sprintf "UpdateBuild -Version \"%s\"" nugetVersion) |> ignore
)

// Generate assembly info files with the right version & up-to-date information
Target "AssemblyInfo" (fun _ ->
    let attrs =
        [ 
            Attribute.Title project
            Attribute.Product project
            Attribute.Company "Daniel Chambers & Contributors"
            Attribute.Copyright "Copyright \169 Daniel Chambers & Contributors 2016"
            Attribute.Version release.AssemblyVersion
            Attribute.FileVersion release.AssemblyVersion
            Attribute.InternalsVisibleTo "FSharp.Azure.Storage.IntegrationTests"
        ] 

    !! "**/AssemblyInfo.fs" |> Seq.iter (fun f -> CreateFSharpAssemblyInfo f attrs)
)


// --------------------------------------------------------------------------------------
// Clean build results & restore NuGet packages

Target "Clean" (fun _ ->
    CleanDirs <| !! "./**/bin/"
    CleanDir "./tools/output"
    CleanDir "./temp"
)

//
//// --------------------------------------------------------------------------------------
//// Build library & test project

let configuration = environVarOrDefault "Configuration" "Release"

Target "Build" (fun () ->
    // Build the rest of the project
    { BaseDirectory = __SOURCE_DIRECTORY__
      Includes = [ project + ".sln" ]
      Excludes = [] } 
    |> MSBuild "" "Build" ["Configuration", "Release"]
    |> Log "AppBuild-Output: ")

// --------------------------------------------------------------------------------------
// Run the unit tests using test runner & kill test runner when complete

Target "RunTests" (fun _ ->
    testAssemblies
    |> Seq.collect (!!)
    |> xUnit2 (fun (p : XUnit2Params) -> 
        { p with
            TimeOut = TimeSpan.FromMinutes 20.
            Parallel = ParallelMode.NoParallelization
            ExcludeTraits = 
                [ if not emulatorTests then yield ("Category", "Emulator")
                  if not remoteTests then yield ("Category", "Remote") ]

            HtmlOutputPath = Some "xunit.html"})
)

FinalTarget "CloseTestRunner" (fun _ ->  
//    ProcessHelper.killProcess "nunit-agent.exe"
    ()
)

//
//// --------------------------------------------------------------------------------------
//// Build a NuGet package

Target "NuGet" (fun _ ->    
    Paket.Pack (fun p -> 
        { p with 
            ToolPath = ".paket/paket.exe" 
            OutputPath = "bin/"
            Version = release.NugetVersion
            ReleaseNotes = toLines release.Notes })
)

Target "NuGetPush" (fun _ -> Paket.Push (fun p -> { p with WorkingDir = "bin/" }))


// Doc generation

Target "GenerateDocs" (fun _ ->
    executeFSIWithArgs "docs/tools" "generate.fsx" ["--define:RELEASE"] [] |> ignore
)

Target "ReleaseDocs" (fun _ ->
    let tempDocsDir = "temp/gh-pages"
    let outputDir = "docs/output"

    CleanDir tempDocsDir
    Repository.cloneSingleBranch "" (gitHome + "/" + gitName + ".git") "gh-pages" tempDocsDir
    
    fullclean tempDocsDir
    ensureDirectory outputDir
    CopyRecursive outputDir tempDocsDir true |> tracefn "%A"
    StageAll tempDocsDir
    Commit tempDocsDir (sprintf "Update generated documentation for version %s" release.NugetVersion)
    Branches.push tempDocsDir
)

// Github Releases

#load "paket-files/fsharp/FAKE/modules/Octokit/Octokit.fsx"
open Octokit

Target "ReleaseGitHub" (fun _ ->
    let remote =
        Git.CommandHelper.getGitResult "" "remote -v"
        |> Seq.filter (fun (s: string) -> s.EndsWith("(push)"))
        |> Seq.tryFind (fun (s: string) -> s.Contains(gitOwner + "/" + gitName))
        |> function None -> gitHome + "/" + gitName | Some (s: string) -> s.Split().[0]

    //StageAll ""
    Git.Commit.Commit "" (sprintf "Bump version to %s" release.NugetVersion)
    Branches.pushBranch "" remote (Information.getBranchName "")

    Branches.tag "" release.NugetVersion
    Branches.pushTag "" remote release.NugetVersion

    let client =
        match Environment.GetEnvironmentVariable "OctokitToken" with
        | null -> 
            let user =
                match getBuildParam "github-user" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> getUserInput "Username: "
            let pw =
                match getBuildParam "github-pw" with
                | s when not (String.IsNullOrWhiteSpace s) -> s
                | _ -> getUserPassword "Password: "

            createClient user pw
        | token -> createClientWithToken token

    // release on github
    client
    |> createDraft gitOwner gitName release.NugetVersion (release.SemVer.PreRelease <> None) release.Notes
    |> releaseDraft
    |> Async.RunSynchronously
)

// --------------------------------------------------------------------------------------
// Run all targets by default. Invoke 'build <Target>' to override

Target "Prepare" DoNothing
Target "PrepareRelease" DoNothing
Target "Default" DoNothing
Target "Release" DoNothing

"Clean"
  ==> "AssemblyInfo"
  ==> "Prepare"
  ==> "Build"
  ==> "RunTests"
  ==> "Default"

"Build"
  ==> "PrepareRelease"
//  ==> "GenerateDocs"
//  ==> "ReleaseDocs"
  ==> "NuGet"
  ==> "NuGetPush"
  ==> "ReleaseGithub"
  ==> "Release"

RunTargetOrDefault "Default"