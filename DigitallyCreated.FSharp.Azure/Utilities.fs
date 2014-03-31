namespace DigitallyCreated.FSharp.Azure

open System;

module private Utilities =
    open Microsoft.FSharp.Reflection
    
    let inline (|?) lhs rhs = (if lhs = null then rhs else lhs)

    let inline isNull o = match o with | null -> true | _ -> false
    let inline isNotNull o = match o with | null -> false | _ -> true

    let notImplemented() = 
        raise (NotImplementedException())
        Unchecked.defaultof<_>

    let runtimeGetUncheckedDefault (t : Type) =
        if t.IsValueType && Nullable.GetUnderlyingType t |> isNull then
            Activator.CreateInstance t
        else
            null

    let tryGet key (dict : System.Collections.Generic.IDictionary<_,_>) = 
        let mutable value = null;
        match dict.TryGetValue (key, &value) with
        | true -> Some value
        | false -> None