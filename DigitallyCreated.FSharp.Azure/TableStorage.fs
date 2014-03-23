namespace DigitallyCreated.FSharp.Azure

    open System;

    module TableStorage =

        open System.Threading.Tasks
        open Microsoft.FSharp.Reflection
        open Microsoft.WindowsAzure.Storage
        open Microsoft.WindowsAzure.Storage.Table
        open Utilities

        type TableEntityIdentifier = { PartitionKey : string; RowKey : string; }
        type OperationResult = { HttpStatusCode : int; Etag : string }

        type private RecordTableEntityWrapper<'T>(record : 'T, identifier, etag) =
            static let recordFields = 
                FSharpType.GetRecordFields typeof<'T> |> Array.map (fun p -> p.Name)
            static let recordReader = 
                FSharpValue.PreComputeRecordReader typeof<'T> >> Seq.map EntityProperty.CreateEntityPropertyFromObject

            member this.Record with get() = record

            interface ITableEntity with
                member val PartitionKey : string = identifier.PartitionKey with get, set
                member val RowKey : string = identifier.RowKey with get, set
                member val ETag : string = etag with get, set
                member val Timestamp : DateTimeOffset = Unchecked.defaultof<_> with get, set

                member this.ReadEntity(properties, operationContext) =
                    notImplemented()

                member this.WriteEntity(operationContext) = 
                    recordReader(record) |> Seq.zip recordFields |> dict

        let private createEntityTableOperation (tableOperation : ITableEntity -> TableOperation) getEntityIdentifier record etag =
            let eId = getEntityIdentifier record
            let wrapper = RecordTableEntityWrapper (record, eId, etag)
            tableOperation wrapper

        let insert getId record = createEntityTableOperation TableOperation.Insert getId record null
        let insertOrMerge getId record = createEntityTableOperation TableOperation.InsertOrMerge getId record null
        let insertOrReplace getId record = createEntityTableOperation TableOperation.InsertOrReplace getId record null
        let merge getId (etag, record) = createEntityTableOperation TableOperation.Merge getId record etag
        let forceMerge getId record = merge getId ("*", record)
        let replace getId (etag, record) = createEntityTableOperation TableOperation.Replace getId record etag
        let forceReplace getId record = replace getId ("*", record)
    
        let inTable (client: CloudTableClient) name operation =
            let table = client.GetTableReference name
            let result = table.Execute operation
            { HttpStatusCode = result.HttpStatusCode; Etag = result.Etag }