# Castle Windsor example

This net472 console application demonstrates explicit endpoint registration and per-message Castle Windsor scopes. It runs on Mono with RabbitMQ and `Mono.Data.Sqlite` installed.

```sh
dotnet restore Bq.Reference.Windsor/Bq.Reference.Windsor.csproj
dotnet build Bq.Reference.Windsor/Bq.Reference.Windsor.csproj --no-restore
mono Bq.Reference.Windsor/bin/Debug/net472/Bq.Reference.Windsor.exe
```

The broker defaults to `amqp://guest:guest@127.0.0.1:5672/`; set `BQ_RABBITMQ_URI` to override it. `BQ_SAMPLE_DB` changes the file path from `bq-windsor.db`. The program creates the database schema, sends one order, waits for its follow-up message to mark it prepared, and shuts down gracefully. It leaves the database and durable RabbitMQ queues in place so they can be inspected.

`Bq.Extensions.CastleWindsor` opens a Windsor scope per message attempt. `WindsorMessaging.Attach` keeps it active through before-commit callbacks. `WindsorMessaging.Handle<T>` resolves a handler registered in the existing container. Within that scope, `IMessageHandlerContext` and `IUniformSession` are resolved from the active delivery. `OrderRepository` shares the delivery's database connection and transaction, while `OrderMessages` uses the same uniform-session interface both in a handler and in the console request. The console request opens `ExecuteTransaction`, writes the initial order state, and resolves a scoped `OrderMessages` that queues the first message in the same transaction. Each handler then commits its own state update and any outgoing messages through the outbox.

The default SQLite ADO.NET connection is `Mono.Data.Sqlite.SqliteConnection`, resolved from Mono at runtime. For a different compatible provider, set `BQ_SQLITE_CONNECTION_TYPE` to its assembly-qualified connection type; its assembly and native dependencies must already be available. The sample connection string uses `Data Source` and `Default Timeout`; adjust `Connection` if a provider needs different options. SQLite is appropriate here as a local reference, not as a distributed database deployment.

This adapter is sample application code, not automatic integration between Bq and Windsor. The application owns the container, registrations and endpoint lifetime. Bq's API and message format are independently implemented and are not binary or wire-compatible with NServiceBus.


[Browse the sample source on GitHub](https://github.com/vivainio/bq/tree/master/Bq.Reference.Windsor).

To use the helper in another host, reference `Bq.Extensions.CastleWindsor` and give `WindsorMessaging` your existing container. Register your handler in Windsor with a scoped lifestyle before starting the endpoint.

```csharp
var messaging = new WindsorMessaging(container);
messaging.Attach(configuration);
messaging.Handle<PlaceOrder>(configuration);
```

Attach the integration before any other incoming behaviors that resolve scoped services. Keep the root container alive until the endpoint stops.

For saga-not-found handling, register an `IHandleSagaNotFound` component with a scoped lifestyle and call `messaging.HandleSagaNotFound(configuration)`. Windsor resolves the handler inside the message scope, so it can use the same scoped services and transaction as an ordinary handler.

`Handle<T>(configuration)` uses the endpoint's event subscription conventions. Pass `subscribe: true` or `subscribe: false` to choose explicitly.

Inject `SynchronizedStorageSession` into a scoped Windsor component to use the active delivery's ADO.NET connection and transaction. Bq owns the transaction and commits after the Windsor scope closes.

Register a saga as a transient Windsor component and call `messaging.RegisterSaga<TSaga, TData>(configuration, mappings => ...)`. Supply the message and timeout mappings during configuration. Windsor resolves and releases the saga during each processing attempt.

For a saga with a public parameterless constructor, `messaging.RegisterSaga<TSaga, TData>(configuration)` reads its `ConfigureHowToFindSaga` mappings and discovers timeout handlers at startup. Bq constructs the mapping instance at startup and resolves runtime instances through Windsor.

For constructor-injected sagas, pass a `definitionFactory` if you want Bq to read `ConfigureHowToFindSaga` at startup. The factory creates a separate instance using dependencies available at startup. Bq disposes that instance if it implements `IDisposable`. If the constructor needs an active delivery, use the explicit mappings overload.

Scoped components can also inject `IMessageProcessingContext` or `IPipelineContext`. Both resolve to the active handler context and share its transaction and context bag.

Bq releases each resolved handler through Windsor after invocation, including saga-not-found handlers. Windsor applies the component's configured lifestyle.
