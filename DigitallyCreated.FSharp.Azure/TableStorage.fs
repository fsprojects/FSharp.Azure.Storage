namespace DigitallyCreated.FSharp.Azure

    open System;

    module TableStorage =

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

            let pk = Expr.Var(Var("pk", typeof<string>))
            let rk = Expr.Var(Var("rk", typeof<string>))
            let var = Var("var", typeof<'T>)
            let recordInitializer = <@ { PartitionKey = %%pk; RowKey = %%rk } @>
            let quotation = recordInitializer.Substitute(fun v -> 
                match v.Name with
                | "pk" -> Some (Expr.PropertyGet (Expr.Var(var), partitionKeyProperty))
                | "rk" -> Some (Expr.PropertyGet (Expr.Var(var), rowKeyProperty))
                | _ -> failwith "Unexpected free variable")
            let quotation = Expr.Cast<'T -> TableEntityIdentifier>(Expr.Lambda(var, quotation))
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
                FSharpType.GetRecordFields typeof<'T> |> Array.map (fun p -> p.Name)
            static let recordReader = 
                FSharpValue.PreComputeRecordReader typeof<'T>

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
                        |> Seq.zip recordFields 
                        |> dict

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