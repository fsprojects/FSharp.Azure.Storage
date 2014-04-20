namespace DigitallyCreated.FSharp.Azure

open System;

module internal Array = 
    let window skip take (arr : 'a[]) =
        seq {
            for i in skip .. min (skip + take - 1) (arr.Length - 1) do yield arr.[i]
        }

module internal Utilities =
    
    open System.Reflection
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

    let getPropertyByAttribute<'T, 'TAttr, 'TReturn when 'TAttr :> Attribute and 'TAttr : null>() =
        let partitionKeyProperties = 
            typeof<'T>.GetProperties() 
            |> Seq.where (fun p -> p.CanRead)
            |> Seq.where (fun p -> p.GetCustomAttribute<'TAttr>() |> isNotNull)
            |> Seq.toList

        match partitionKeyProperties with
        | h :: [] when h.PropertyType = typeof<'TReturn> -> h
        | h :: [] -> failwithf "The property %s on type %s that is marked with %s is not of type %s" h.Name typeof<'T>.Name typeof<'TAttr>.Name typeof<'TReturn>.Name 
        | h :: t -> failwithf "The type %s contains more than one property with %s" typeof<'T>.Name typeof<'TAttr>.Name
        | [] -> failwithf "The type %s does not contain a property with %s" typeof<'T>.Name typeof<'TAttr>.Name