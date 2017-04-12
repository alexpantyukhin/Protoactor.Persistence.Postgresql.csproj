using System;
using Proto.Persistence;

namespace Protoactor.Persistence.Postgresql
{
    public class PostgresqlProvider : IProvider
    {
        private readonly string _connectionString;
        private readonly bool _autoCreateTables;
        private readonly string _useTablesWithPrefix;
        private readonly string _useTablesWithSchema;

        public PostgresqlProvider(string connectionString, bool autoCreateTables = false, string useTablesWithPrefix = "", string useTablesWithSchema = "dbo")
        {
            _connectionString = connectionString;
            _autoCreateTables = autoCreateTables;
            _useTablesWithPrefix = useTablesWithPrefix;
            _useTablesWithSchema = useTablesWithSchema;
        }

        public IProviderState GetState()
        {
            return new PostgresqlProviderState(_connectionString, _autoCreateTables, _useTablesWithPrefix, _useTablesWithSchema);
        }
    }
}
