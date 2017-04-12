using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Proto.Persistence;
using Npgsql;
using Newtonsoft.Json;

namespace Protoactor.Persistence.Postgresql
{
    public class PostgresqlProviderState : IProviderState
    {
        private readonly string _connectionString;
        private readonly string _tableSnapshots = "Snapshots";
        private readonly string _tableEvents = "Events";
        private readonly string _tableSchema;

        public PostgresqlProviderState(string connectionString, bool autoCreateTables, string useTablesWithPrefix, string useTablesWithSchema)
        {
            _connectionString = connectionString;
            _tableSchema = useTablesWithSchema;

            if (string.IsNullOrEmpty(useTablesWithPrefix))
            {
                _tableSnapshots = $"{useTablesWithPrefix}_Snapshots";
                _tableEvents = $"{useTablesWithPrefix}_Events";
            }

            if (autoCreateTables)
            {
                AutoCreateSnapshotTableInDatabaseAsync().Wait();
                AutoCreateEventTableInDatabaseAsync().Wait();
            }
        }

        private async Task ExecuteNonQueryAsync(string sql, List<NpgsqlParameter> parameters = null)
        {
            try
            {
                using (var connection = new NpgsqlConnection(_connectionString))
                using (var command = new NpgsqlCommand(sql, connection))
                {
                    await connection.OpenAsync();

                    using (var tx = connection.BeginTransaction())
                    {
                        command.Transaction = tx;

                        if (parameters?.Count > 0)
                        {
                            foreach (var p in parameters)
                            {
                                command.Parameters.Add(p);
                            }
                        }

                        await command.ExecuteNonQueryAsync();

                        tx.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private async Task AutoCreateSnapshotTableInDatabaseAsync()
        {
            var sql = $@"
                IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tables 
                            WHERE  schemaname = '{_tableSchema}'
                            AND    tablename  = '{_tableSnapshots}')
                THEN
                    CREATE TABLE {_tableSchema}.{_tableSnapshots} (
                        Id VARCHAR(255) CONSTRAINT PK_{_tableSnapshots} PRIMARY KEY,
                        ActorName VARCHAR(255) NOT NULL,,
                        EventIndex BIGINT NOT NULL,
                        EventData JSONB NOT NULL,
                        Created TIMESTAMP NOT NULL CONSTRAINT [DF_{_tableSnapshots}_Created] DEFAULT (now() at time zone 'utc')
                    );
                    
                    CREATE INDEX IX_{_tableSnapshots}_ActorNameAndSnapshotIndex ON {_tableSchema}.{_tableSnapshots}(ActorName ASC, EventIndex ASC);
                END IF;
            ";

            await ExecuteNonQueryAsync(sql);
        }

        private async Task AutoCreateEventTableInDatabaseAsync()
        {
            var sql = $@"
                IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_tables 
                            WHERE  schemaname = '{_tableSchema}'
                            AND    tablename  = '{_tableEvents}')
                THEN
                    CREATE TABLE {_tableSchema}.{_tableEvents} (
                        Id VARCHAR(255) CONSTRAINT PK_{_tableEvents} PRIMARY KEY,
                        ActorName VARCHAR(255) NOT NULL,,
                        EventIndex BIGINT NOT NULL,
                        EventData JSONB NOT NULL,
                        Created TIMESTAMP NOT NULL CONSTRAINT [DF_{_tableEvents}_Created] DEFAULT (now() at time zone 'utc')
                    );
                    
                    CREATE INDEX IX_{_tableEvents}_ActorNameAndEventIndex ON {_tableSchema}.{_tableEvents}(ActorName ASC, EventIndex ASC);
                END IF;
            ";

            await ExecuteNonQueryAsync(sql);
        }

        public async Task GetEventsAsync(string actorName, long indexStart, Action<object> callback)
        {
            var sql = $@"SELECT EventData FROM {_tableSchema}.{_tableEvents} WHERE ActorName = @ActorName AND EventIndex >= @EventIndex ORDER BY EventIndex ASC;";

            try
            {
                using (var connection = new NpgsqlConnection(_connectionString))
                using (var command = new NpgsqlCommand(sql, connection))
                {
                    await connection.OpenAsync();

                    command.Parameters.Add(new NpgsqlParameter()
                    {
                        ParameterName = "ActorName",
                        NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar,
                        Value = actorName
                    });

                    command.Parameters.Add(new NpgsqlParameter()
                    {
                        ParameterName = "EventIndex",
                        NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint,
                        Value = indexStart
                    });

                    var eventReader = await command.ExecuteReaderAsync();

                    while(await eventReader.ReadAsync())
                    {
                        callback(JsonConvert.DeserializeObject<object>(eventReader["EventData"].ToString(), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto }));
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<Tuple<object, long>> GetSnapshotAsync(string actorName)
        {
            long snapshotIndex = 0;
            object snapshotData = null;

            var sql = $@"SELECT SnapshotIndex, SnapshotData FROM {_tableSchema}.{_tableSnapshots} WHERE ActorName = @ActorName ORDER BY SnapshotIndex DESC LIMIT 1;";

            try
            {
                using (var connection = new NpgsqlConnection(_connectionString))
                using (var command = new NpgsqlCommand(sql, connection))
                {
                    await connection.OpenAsync();

                    command.Parameters.Add(new NpgsqlParameter()
                    {
                        ParameterName = "ActorName",
                        NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar,
                        Value = actorName
                    });

                    var snapshotReader = await command.ExecuteReaderAsync();

                    while (await snapshotReader.ReadAsync())
                    {
                        snapshotIndex = Convert.ToInt64(snapshotReader["SnapshotIndex"]);
                        snapshotData = JsonConvert.DeserializeObject<object>(snapshotReader["SnapshotData"].ToString(), new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.Auto });
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return new Tuple<object, long>(snapshotData, snapshotIndex);
        }

        public async Task PersistEventAsync(string actorName, long index, object @event)
        {
            var item = new Event(actorName, index, @event);

            var sql = $@"INSERT INTO {_tableSchema}.{_tableEvents} (Id, ActorName, EventIndex, EventData) VALUES (@Id, @ActorName, @EventIndex, @EventData);";

            var parameters = new List<NpgsqlParameter>
            {
                new NpgsqlParameter()
                {
                    ParameterName = "Id",
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar,
                    Value = item.Id
                },
                new NpgsqlParameter()
                {
                    ParameterName = "ActorName",
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar,
                    Value = item.ActorName
                },
                new NpgsqlParameter()
                {
                    ParameterName = "EventIndex",
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint,
                    Value = item.EventIndex
                },
                new NpgsqlParameter()
                {
                    ParameterName = "EventData",
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text,
                    Value =  JsonConvert.SerializeObject(item.EventData, new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All })
                }
            };

            await ExecuteNonQueryAsync(sql, parameters);
        }

        public async Task PersistSnapshotAsync(string actorName, long index, object snapshot)
        {
            var item = new Snapshot(actorName, index, snapshot);

            var sql = $@"INSERT INTO {_tableSchema}.{_tableSnapshots} (Id, ActorName, SnapshotIndex, SnapshotData) VALUES (@Id, @ActorName, @SnapshotIndex, @SnapshotData);";

            var parameters = new List<NpgsqlParameter>
            {
                new NpgsqlParameter()
                {
                    ParameterName = "Id",
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar,
                    Value = item.Id
                },
                new NpgsqlParameter()
                {
                    ParameterName = "ActorName",
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar,
                    Value = item.ActorName
                },
                new NpgsqlParameter()
                {
                    ParameterName = "SnapshotIndex",
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar,
                    Value = item.SnapshotIndex
                },
                new NpgsqlParameter()
                {
                    ParameterName = "SnapshotData",
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar,
                    Value = JsonConvert.SerializeObject(item.SnapshotData, new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All })
                }
            };

            await ExecuteNonQueryAsync(sql, parameters);
        }

        public async Task DeleteEventsAsync(string actorName, long inclusiveToIndex)
        {
            var sql = $@"DELETE FROM {_tableSchema}.{_tableEvents} WHERE ActorName = @ActorName AND EventIndex <= @EventIndex;";

            var parameters = new List<NpgsqlParameter>
            {
                new NpgsqlParameter()
                {
                    ParameterName = "ActorName",
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar,
                    Value = actorName
                },
                new NpgsqlParameter()
                {
                    ParameterName = "EventIndex",
                    NpgsqlDbType =  NpgsqlTypes.NpgsqlDbType.Bigint,
                    Value = inclusiveToIndex
                }
            };

            await ExecuteNonQueryAsync(sql, parameters);
        }

        public async Task DeleteSnapshotsAsync(string actorName, long inclusiveToIndex)
        {
            var sql = $@"DELETE FROM {_tableSchema}.{_tableSnapshots} WHERE ActorName = @ActorName AND SnapshotIndex <= @SnapshotIndex;";

            var parameters = new List<NpgsqlParameter>
            {
                new NpgsqlParameter()
                {
                    ParameterName = "ActorName",
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar,
                    Value = actorName
                },
                new NpgsqlParameter()
                {
                    ParameterName = "SnapshotIndex",
                    NpgsqlDbType =  NpgsqlTypes.NpgsqlDbType.Bigint,
                    Value = inclusiveToIndex
                }
            };

            await ExecuteNonQueryAsync(sql, parameters);
        }
    }
}
