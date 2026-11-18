# Castle Windsor example

This net472 console application demonstrates explicit endpoint registration and per-message Castle Windsor scopes. It runs on Mono with RabbitMQ and `Mono.Data.Sqlite` installed.

```sh
dotnet restore Bq.Reference.Windsor/Bq.Reference.Windsor.csproj
dotnet build Bq.Reference.Windsor/Bq.Reference.Windsor.csproj --no-restore
mono Bq.Reference.Windsor/bin/Debug/net472/Bq.Reference.Windsor.exe
```

The broker defaults to `amqp://guest:guest@127.0.0.1:5672/`; set `BQ_RABBITMQ_URI` to override it. `BQ_SAMPLE_DB` changes the file path from `bq-windsor.db`. The program creates the database schema, sends one order, waits for its follow-up message to mark it prepared, and shuts down gracefully. It leaves the database and durable RabbitMQ queues in place so they can be inspected.

`WindsorHandlerScope` creates and disposes a Windsor scope per handler attempt. Within that scope, `IMessageHandlerContext` and `IUniformSession` are resolved from the active delivery. `OrderRepository` shares the delivery's database connection and transaction, while `OrderMessages` uses the same uniform-session interface both in a handler and in the console request. The console request opens `ExecuteTransaction`, writes the initial order state, and resolves a scoped `OrderMessages` that queues the first message in the same transaction. Each handler then commits its own state update and any outgoing messages through the outbox.

The default SQLite ADO.NET connection is `Mono.Data.Sqlite.SqliteConnection`, resolved from Mono at runtime. For a different compatible provider, set `BQ_SQLITE_CONNECTION_TYPE` to its assembly-qualified connection type; its assembly and native dependencies must already be available. The sample connection string uses `Data Source` and `Default Timeout`; adjust `Connection` if a provider needs different options. SQLite is appropriate here as a local reference, not as a distributed database deployment.

This adapter is sample application code, not automatic integration between Bq and Windsor. The application owns the container, registrations and endpoint lifetime. Bq's API and message format are independently implemented and are not binary or wire-compatible with NServiceBus.


[Browse the sample source on GitHub](https://github.com/vivainio/bq/tree/master/Bq.Reference.Windsor).
