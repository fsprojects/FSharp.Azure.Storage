### 4.0.0
* Switched from using the WindowsAzure.Storage package, which is deprecated, to the Microsoft.Azure.Cosmos.Table package. NOTE: This is a breaking change, but simple to fix; just update namespaces (see PR#39) (Thanks @JohnDoeKyrgyz).

### 3.3.1
* Fixed using option type with int64. (Thanks @CameronAavik)

### 3.3.0
* Support for record fields typed as union types that do not have fields. (Thanks @aaronpowell)

### 3.2.0
* New FSharp.Azure.Storage.Table.Task module that contains Task<T> implementations of the async functions. (Thanks @coolya)

### 3.1.0
* New Etag and Timestamp attributes that can be applied to record fields; the row's ETag and Timestamp will be written into those fields on query. These fields are ignored when writing back into table storage. (Thanks @coolya)
* The System.Uri type is now supported as a record field type. (Thanks @coolya)

### 3.0.0
* .NET Standard 2.0 support. NOTE: All sync functions (eg. fromTable, etc) are now sync-over-async functions. See https://github.com/Azure/azure-storage-net/issues/367
* Fixed bug in fromTableAsync where take count wasn't respected

### 2.2.0
* Removed dependency on FSPowerPack; Unquote is used to perform expression tree evaluation

### 2.1.0
* DateTime can now be used as a type for properties.
* Renamed project from FSharp.Azure to FSharp.Azure.Storage.
* Namespaces and assemblies have been changed from DigitallyCreated.FSharp.Azure to FSharp.Azure.Storage
* TableStorage module has been renamed to Table