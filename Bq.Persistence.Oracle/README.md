# Oracle persistence

`Bq.Persistence.Oracle` targets .NET Standard 2.0 and uses Oracle 12c or later SQL. It implements transactional inbox deduplication, outbox and scheduled delivery, saga storage, and recovery. The application supplies either a Devart dotConnect or Oracle ODP.NET connection. The adapter has no compile-time dependency on either driver.

Apply [001_create_bq_tables.sql](migrations/001_create_bq_tables.sql) once through your schema migration process. Use a connection whose current schema contains those tables. Set the endpoint name to the same value used for `EndpointConfiguration.Name`.

```csharp
using Bq.Persistence.Oracle;

// Existing Devart host
configuration.Persistence = new OraclePersistence(
    () => new Devart.Data.Oracle.OracleConnection(connectionString),
    configuration.Name);

// Or ODP.NET
configuration.Persistence = new OraclePersistence(
    () => new Oracle.ManagedDataAccess.Client.OracleConnection(connectionString),
    configuration.Name);
```

Choose the driver that your application data layer uses. The factory returns a new closed connection on each call, and Bq owns its lifetime. Bq exposes the open connection and transaction through `IMessageHandlerContext.PersistenceSession` and `ITransactionalSession`. For EF6, construct the context with that exact connection and `contextOwnsConnection: false`, then call `context.Database.UseTransaction(session.Transaction)`. Let Bq commit. Execute application writes or call `SaveChanges` before the handler or before-commit callback completes.

The dispatcher holds an Oracle row lock while publishing and records delivery after broker confirmation. A failed publish leaves the record pending. Delivery remains at least once if the broker confirms and the database commit subsequently fails. The adapter does not schedule retention cleanup.

## Integration check

Apply the schema to a disposable test database. Run the existing messaging test executable with these environment variables, keeping driver dependencies alongside the assembly. The Oracle checks do not require RabbitMQ.

- `BQ_ORACLE_CONNECTION_STRING` supplies the connection string.
- `BQ_ORACLE_DRIVER_ASSEMBLY` supplies the path to the driver assembly.
- `BQ_ORACLE_CONNECTION_TYPE` supplies `Devart.Data.Oracle.OracleConnection` or `Oracle.ManagedDataAccess.Client.OracleConnection`.

Run the checks under a runtime supported by the selected driver. Devart may require its normal application licensing configuration.
