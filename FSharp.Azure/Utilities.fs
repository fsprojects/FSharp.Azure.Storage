namespace DigitallyCreated.FSharp.Azure

open System;

module internal Array = 
    let window skip take (arr : 'a[]) =
        seq {
            for i in skip .. min (skip + take - 1) (arr.Length - 1) do yield arr.[i]
        }

module internal Utilities =
    open Microsoft.FSharp.Reflection
    
    let inline (|?) (lhs: 'a option) rhs = (if lhs.IsSome then lhs.Value else rhs)
    let inline toNullable (opt: 'a option) = if opt.IsSome then Nullable(opt.Value) else Nullable()
    let inline toNullRef (opt: 'a option) = if opt.IsSome then opt.Value else null
    let inline toOption o = match o with | null -> None | _ -> Some(o)
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
