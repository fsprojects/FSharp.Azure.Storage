namespace DigitallyCreated.FSharp.Azure

open System;

module private Utilities =
    open Microsoft.FSharp.Reflection
    
    let inline (|?) lhs rhs = (if lhs = null then rhs else lhs)

    let notImplemented() = 
        raise (NotImplementedException())
        Unchecked.defaultof<_>