namespace System
open System.Reflection
open System.Runtime.CompilerServices

[<assembly: AssemblyTitleAttribute("FSharp.Azure.Storage")>]
[<assembly: AssemblyProductAttribute("FSharp.Azure.Storage")>]
[<assembly: AssemblyCompanyAttribute("Daniel Chambers & Contributors")>]
[<assembly: AssemblyCopyrightAttribute("Copyright © Daniel Chambers & Contributors 2015")>]
[<assembly: AssemblyVersionAttribute("2.1.0")>]
[<assembly: AssemblyFileVersionAttribute("2.1.0")>]
[<assembly: InternalsVisibleToAttribute("FSharp.Azure.Storage.IntegrationTests")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "2.1.0"
