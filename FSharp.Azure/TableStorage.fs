namespace DigitallyCreated.FSharp.Azure

    open System;

    module TableStorage =

        open System.Collections.Generic
        open System.Linq
        open System.Reflection
        open System.Threading.Tasks
        open Microsoft.FSharp.Linq.QuotationEvaluation
        open Microsoft.FSharp.Quotations
        open Microsoft.FSharp.Quotations.Patterns
        open Microsoft.FSharp.Reflection
        open Microsoft.WindowsAzure.Storage
        open Microsoft.WindowsAzure.Storage.Table
        open Utilities

        type TableEntityIdentifier = { PartitionKey : string; RowKey : string; }
        type OperationResult = { HttpStatusCode : int; Etag : string }
        type EntityMetadata = { Etag : string; Timestamp : DateTimeOffset }

        type ITableIdentifiable = 
            abstract member GetIdentifier : unit -> TableEntityIdentifier

        [<AllowNullLiteralAttribute>]
        type PartitionKeyAttribute () = inherit Attribute()
        
        [<AllowNullLiteralAttribute>]
        type RowKeyAttribute () = inherit Attribute()

        let private getPropertyByAttribute<'T, 'TAttr, 'TReturn when 'TAttr :> Attribute and 'TAttr : null>() =
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

        let private buildIdentiferFromAttributesFunc<'T>() =
            let partitionKeyProperty = getPropertyByAttribute<'T, PartitionKeyAttribute, string>()
            let rowKeyProperty = getPropertyByAttribute<'T, RowKeyAttribute, string>()

            let var = Var("o", typeof<'T>)
            let pk = Expr.PropertyGet (Expr.Var(var), partitionKeyProperty)
            let rk = Expr.PropertyGet (Expr.Var(var), rowKeyProperty)
            
            let recordInitializer = <@ { PartitionKey = %%pk; RowKey = %%rk } @>

            let quotation = Expr.Cast<'T -> TableEntityIdentifier>(Expr.Lambda(var, recordInitializer))
            quotation.Compile()()


        [<AbstractClass; Sealed>]
        type TableIdentifier<'T> private () =
            static let getFromAttributes = lazy buildIdentiferFromAttributesFunc<'T>()

            static let defaultGetIdentifier record =
                match box record with
                | :? ITableIdentifiable as identifiable -> identifiable.GetIdentifier()
                | _ -> getFromAttributes.Value(record)
                    
            static member GetIdentifier = ref defaultGetIdentifier


        type private RecordTableEntityWrapper<'T>(record : 'T, identifier, etag) =
            static let recordFields = 
                FSharpType.GetRecordFields typeof<'T>
            static let recordReader = 
                FSharpValue.PreComputeRecordReader typeof<'T>
            static let recordWriter = 
                FSharpValue.PreComputeRecordConstructor typeof<'T> >> (fun o -> o :?> 'T)

            static member ResolveEntity (pk : string) (rk : string) (timestamp: DateTimeOffset) (properties : IDictionary<string, EntityProperty>) (etag : string) =
                let propValues = 
                    recordFields 
                        |> Seq.map (fun f -> 
                            match properties |> tryGet f.Name with
                            | Some prop -> 
                                match prop.PropertyAsObject with
                                | null -> runtimeGetUncheckedDefault f.PropertyType
                                | value when value.GetType() <> f.PropertyType -> 
                                    failwithf "The property %s on type %s of type %s has deserialized as the incorrect type %s" f.Name typeof<'T>.Name f.PropertyType.Name (value.GetType().Name)
                                | value -> value
                            | None -> runtimeGetUncheckedDefault f.PropertyType)
                        |> Seq.toArray
                (recordWriter propValues), { Etag = etag; Timestamp = timestamp }

            member this.Record with get() = record

            interface ITableEntity with
                member val PartitionKey : string = identifier.PartitionKey with get, set
                member val RowKey : string = identifier.RowKey with get, set
                member val ETag : string = etag with get, set
                member val Timestamp : DateTimeOffset = Unchecked.defaultof<_> with get, set

                member this.ReadEntity(properties, operationContext) =
                    notImplemented()

                member this.WriteEntity(operationContext) = 
                    record 
                        |> recordReader
                        |> Seq.map EntityProperty.CreateEntityPropertyFromObject 
                        |> Seq.zip (recordFields |> Seq.map (fun p -> p.Name))
                        |> dict

        type EntityQuery<'T> = 
            { Filter : string
              TakeCount : int option }
            static member get_Zero() : EntityQuery<'T> = 
                { Filter = ""; TakeCount = None }
            static member (+) (left : EntityQuery<'T>, right : EntityQuery<'T>) =
                let filter =
                    match left.Filter, right.Filter with
                    | "", "" -> ""
                    | l, "" -> l
                    | "", r -> r
                    | l, r -> TableQuery.CombineFilters (left.Filter, "and", right.Filter)
                let takeCount = 
                    match left.TakeCount, right.TakeCount with
                    | Some l, Some r -> Some (min l r)
                    | Some l, None -> Some l
                    | None, Some r -> Some r
                    | None, None -> None

                { Filter = filter; TakeCount = takeCount }

            member this.ToTableQuery() = 
                TableQuery (FilterString = this.Filter, TakeCount = (this.TakeCount |> toNullable))
                

        module Query = 
            open DerivedPatterns

            type SystemProperties =
                { PartitionKey : string
                  RowKey : string
                  Timestamp : DateTimeOffset }

            type private Comparison =
                | Equals
                | GreaterThan
                | GreaterThanOrEqual
                | LessThan
                | LessThanOrEqual
                | NotEqual
                member this.CommutativeInvert() = 
                    match this with
                    | GreaterThan -> LessThan
                    | GreaterThanOrEqual -> LessThanOrEqual
                    | LessThan -> GreaterThan
                    | LessThanOrEqual -> GreaterThanOrEqual
                    | Equals -> Equals
                    | NotEqual -> NotEqual
            
            let private toOperator comparison = 
                match comparison with
                | Equals -> QueryComparisons.Equal
                | GreaterThan -> QueryComparisons.GreaterThan
                | GreaterThanOrEqual -> QueryComparisons.GreaterThanOrEqual
                | LessThan -> QueryComparisons.LessThan
                | LessThanOrEqual -> QueryComparisons.LessThanOrEqual
                | NotEqual -> QueryComparisons.NotEqual

            let private notFilter filter =
                sprintf "not (%s)" filter

            let private (|ComparisonOp|_|) (expr : Expr) =
                match expr with
                | SpecificCall <@ (=) @> (_, _, [left; right]) -> Some (ComparisonOp Equals, left, right)
                | SpecificCall <@ (>) @> (_, _, [left; right]) -> Some (ComparisonOp GreaterThan, left, right)
                | SpecificCall <@ (>=) @> (_, _, [left; right]) -> Some (ComparisonOp GreaterThanOrEqual, left, right)
                | SpecificCall <@ (<) @> (_, _, [left; right]) -> Some (ComparisonOp LessThan, left, right)
                | SpecificCall <@ (<=) @> (_, _, [left; right]) -> Some (ComparisonOp LessThanOrEqual, left, right)
                | SpecificCall <@ (<>) @> (_, _, [left; right]) -> Some (ComparisonOp NotEqual, left, right)
                | _ -> None

            let private (|PropertyComparison|_|) (expr : Expr) =
                match expr with
                | ComparisonOp (op, PropertyGet (Some (Var(v)), prop, []), valExpr) -> 
                    Some (PropertyComparison (v, prop, op, valExpr))
                | ComparisonOp (op, valExpr, PropertyGet (Some (Var(v)), prop, [])) -> 
                    Some (PropertyComparison (v, prop, op.CommutativeInvert(), valExpr))
                | PropertyGet (Some (Var(v)), prop, []) when prop.PropertyType = typeof<bool> -> 
                    Some (PropertyComparison (v, prop, Equals, Expr.Value(true)))
                | SpecificCall <@ not @> (None, _, [ PropertyGet (Some (Var(v)), prop, []) ]) when prop.PropertyType = typeof<bool> -> 
                    Some (PropertyComparison (v, prop, Equals, Expr.Value(false)))
                | _ -> None

            let private (|ComparisonValue|_|) (expr : Expr) =
                match expr with
                | Value (o, t) -> Some(ComparisonValue (o))
                | expr when expr.GetFreeVars().Any() -> failwithf "Cannot evaluate %A to a comparison value as it contains free variables" expr
                | expr -> Some(ComparisonValue (expr.EvalUntyped()))

            let private generateFilterCondition type' propertyName op (value : obj) = 
               match type' with
                | t when t = typeof<string> -> TableQuery.GenerateFilterCondition (propertyName, op |> toOperator, value :?> string)
                | t when t = typeof<byte[]> -> TableQuery.GenerateFilterConditionForBinary (propertyName, op |> toOperator, value :?> byte[])
                | t when t = typeof<bool> -> TableQuery.GenerateFilterConditionForBool (propertyName, op |> toOperator, value :?> bool)
                | t when t = typeof<DateTimeOffset> -> TableQuery.GenerateFilterConditionForDate (propertyName, op |> toOperator, value :?> DateTimeOffset)
                | t when t = typeof<double> -> TableQuery.GenerateFilterConditionForDouble (propertyName, op |> toOperator, value :?> double)
                | t when t = typeof<Guid> -> TableQuery.GenerateFilterConditionForGuid (propertyName, op |> toOperator, value :?> Guid)
                | t when t = typeof<int> -> TableQuery.GenerateFilterConditionForInt (propertyName, op |> toOperator, value :?> int)
                | t when t = typeof<int64> -> TableQuery.GenerateFilterConditionForLong (propertyName, op |> toOperator, value :?> int64)
                | t -> failwithf "Unexpected property type %s for property %s" t.Name propertyName

            let private isPropertyComparisonAgainstBool expr =
                match expr with
                | PropertyComparison (_, prop, _, _) when prop.PropertyType = typeof<bool> -> true
                | _ -> false

            let private buildPropertyFilter entityVar sysPropVar expr =
                let rec buildPropertyFilterRec expr = 
                    match expr with
                    | AndAlso (left, right) -> 
                        TableQuery.CombineFilters(buildPropertyFilterRec left, "and", buildPropertyFilterRec right)
                    | OrElse (left, right) -> 
                        TableQuery.CombineFilters(buildPropertyFilterRec left, "or", buildPropertyFilterRec right)
                    | SpecificCall <@ not @> (None, _, [nottedExpr]) when not (nottedExpr |> isPropertyComparisonAgainstBool) -> 
                        notFilter (buildPropertyFilterRec nottedExpr)
                    | PropertyComparison (v, prop, op, ComparisonValue (value)) ->
                        if v <> entityVar && v <> sysPropVar then
                            failwithf "Comparison (%A) to property (%s) on value that is not the function parameter (%s)" op prop.Name v.Name
                        generateFilterCondition prop.PropertyType prop.Name op value
                    | _ -> failwithf "Unable to understand expression: %A" expr
                buildPropertyFilterRec expr

            let private makePropertyFilter (expr : Expr<'T -> SystemProperties -> bool>) =
                if expr.GetFreeVars().Any() then
                    failwithf "The expression %A contains free variables." expr
                match expr with
                | Lambda (entityVar, Lambda (sysPropVar, expr)) -> buildPropertyFilter entityVar sysPropVar expr
                | _ -> failwith "Unexpected expression; lambda not found"


            let all<'T> : EntityQuery<'T> = EntityQuery.get_Zero()

            let where (expr : Expr<'T -> SystemProperties -> bool>) (query : EntityQuery<'T>) = 
                [query; { Filter = expr |> makePropertyFilter; TakeCount = None };] |> List.reduce (+)

            let take count (query : EntityQuery<'T>) =
                [query; { Filter = ""; TakeCount = Some count };] |> List.reduce (+)
        

        let private createEntityTableOperation (tableOperation : ITableEntity -> TableOperation) record etag =
            match box record with
                | :? ITableEntity as entity -> tableOperation entity
                | _ -> 
                    let eId = !(TableIdentifier.GetIdentifier) record
                    RecordTableEntityWrapper (record, eId, etag) |> tableOperation

        let insert record = createEntityTableOperation TableOperation.Insert record null
        let insertOrMerge record = createEntityTableOperation TableOperation.InsertOrMerge record null
        let insertOrReplace record = createEntityTableOperation TableOperation.InsertOrReplace record null
        let merge (record, etag) = createEntityTableOperation TableOperation.Merge record etag
        let forceMerge record = merge (record, "*")
        let replace (record, etag) = createEntityTableOperation TableOperation.Replace record etag
        let forceReplace record = replace (record, "*")

        let inTable (client: CloudTableClient) name operation =
            let table = client.GetTableReference name
            let result = table.Execute operation
            { HttpStatusCode = result.HttpStatusCode; Etag = result.Etag }

        let inTableAsync (client: CloudTableClient) name operation = 
            async {
                let table = client.GetTableReference name
                let! result = table.ExecuteAsync operation |> Async.AwaitTask
                return { HttpStatusCode = result.HttpStatusCode; Etag = result.Etag }
            }
            
        let fromTable (client: CloudTableClient) name (query : EntityQuery<'T>) =
            let table = client.GetTableReference name
            let tableQuery = query.ToTableQuery()
            table.ExecuteQuery<'T * EntityMetadata>(tableQuery, RecordTableEntityWrapper.ResolveEntity)

        let fromTableSegmented (client: CloudTableClient) name continuationToken (query : EntityQuery<'T>) =
            let table = client.GetTableReference name
            let tableQuery = query.ToTableQuery()
            let result = table.ExecuteQuerySegmented<'T * EntityMetadata>(tableQuery, RecordTableEntityWrapper.ResolveEntity, continuationToken |> toNullRef)
            result.Results, result.ContinuationToken |> toOption

        let fromTableSegmentedAsync (client: CloudTableClient) name continuationToken (query : EntityQuery<'T>) =
            let table = client.GetTableReference name
            let tableQuery = query.ToTableQuery()
            async {
                let! result = table.ExecuteQuerySegmentedAsync<'T * EntityMetadata>(tableQuery, RecordTableEntityWrapper.ResolveEntity, continuationToken |> toNullRef) |> Async.AwaitTask
                return result.Results, result.ContinuationToken |> toOption
            }

        let fromTableAsync (client: CloudTableClient) name (query : EntityQuery<'T>) =
            let rec getSegmentAsync continutationToken resultsList =
                async {
                    let! result, furtherContinuation = query |> fromTableSegmentedAsync client name continutationToken
                    match furtherContinuation with
                    | Some _ -> return! result :: resultsList |> getSegmentAsync furtherContinuation
                    | None -> return result :: resultsList
                }
            async {
                let! resultsList = getSegmentAsync None []
                return resultsList |> List.rev |> Seq.concat
            }
