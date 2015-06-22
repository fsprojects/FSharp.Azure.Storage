namespace DigitallyCreated.FSharp.Azure

open System;

module internal Seq =
    let split n =
        Seq.fold (fun (count, currSeq, seqs) item ->
            if count = n
            then 1, item |> Seq.singleton, currSeq |> Seq.singleton |> Seq.append seqs
            else count + 1, item |> Seq.singleton |> Seq.append currSeq, seqs)
            (0, Seq.empty, Seq.empty)
        >> (fun (_, currSeq, seqs) -> currSeq |> Seq.singleton |> Seq.append seqs)

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
        let properties = 
            typeof<'T>.GetProperties(BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance ||| BindingFlags.Static) 
            |> Seq.where (fun p -> p.CanRead)
            |> Seq.where (fun p -> p.GetCustomAttribute<'TAttr>() |> isNotNull)
            |> Seq.toList

        match properties with
        | h :: [] when h.PropertyType = typeof<'TReturn> -> h
        | h :: [] -> failwithf "The property %s on type %s that is marked with %s is not of type %s" h.Name typeof<'T>.Name typeof<'TAttr>.Name typeof<'TReturn>.Name 
        | h :: t -> failwithf "The type %s contains more than one property with %s" typeof<'T>.Name typeof<'TAttr>.Name
        | [] -> failwithf "The type %s does not contain a property with %s" typeof<'T>.Name typeof<'TAttr>.Name

    
    let inline private isOptionType (t : Type) =
        t.IsGenericType && t.GetGenericTypeDefinition() = typedefof<obj option>

    let getUnderlyingTypeIfOption (t : Type) = 
        if t |> isOptionType then
            t.GetGenericArguments().[0]
        else
            t

    let unwrapIfOption (o : obj) =
        match o with
        | null -> null
        | :? (string option) as opt -> opt.Value :> obj
        | :? (byte[] option) as opt -> opt.Value :> obj
        | :? (bool option) as opt -> opt.Value :> obj
        | :? (DateTimeOffset option) as opt -> opt.Value :> obj
        | :? (double option) as opt -> opt.Value :> obj
        | :? (Guid option) as opt -> opt.Value :> obj
        | :? (int option) as opt -> opt.Value :> obj
        | other -> other

    let wrapIfOption (t : Type) (o : obj) =
        if t |> isOptionType then
            match o with
            | null -> null
            | :? string as v -> Some v :> obj
            | :? (byte[]) as v -> Some v :> obj
            | :? bool as v -> Some v :> obj
            | :? DateTimeOffset as v -> Some v :> obj
            | :? double as v -> Some v :> obj
            | :? Guid as v -> Some v :> obj
            | :? int as v -> Some v :> obj
            | other -> other
        else o