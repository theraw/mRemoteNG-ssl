using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace mRemoteNG.Config.DatabaseConnectors
{
    public class MySqlDatabaseConnector : IDatabaseConnector
    {
        private DbConnection _dbConnection;
        private string _dbConnectionString;
        private readonly string _dbHost;
        private readonly string _dbPort;
        private readonly string _dbName;
        private readonly string _dbUsername;
        private readonly string _dbPassword;

        public DbConnection DbConnection()
        {
            return _dbConnection;
        }

        public DbCommand DbCommand(string dbCommand)
        {
            return new MySqlCommand(dbCommand, (MySqlConnection)_dbConnection);
        }

        public bool IsConnected => _dbConnection != null && _dbConnection.State == ConnectionState.Open;

        public MySqlDatabaseConnector(string host, string database, string username, string password)
        {
            string[] hostParts = host.Split(new char[] { ':' }, 2);
            _dbHost = hostParts[0];
            _dbPort = (hostParts.Length == 2) ? hostParts[1] : "3306";
            _dbName = database;
            _dbUsername = username;
            _dbPassword = password;
            Initialize();
        }

        private void Initialize()
        {
            BuildSqlConnectionString();
            _dbConnection = new MySqlConnection(_dbConnectionString);
        }

        private void BuildSqlConnectionString()
        {
            _dbConnectionString = $"server={_dbHost};user={_dbUsername};database={_dbName};port={_dbPort};password={_dbPassword};SslMode=Required;";
        }

        public void Connect()
        {
            try
            {
                _dbConnection.Open();
                Console.WriteLine("Connection opened successfully.");
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"MySQL Error: {ex.Message}");
                throw; // Re-throw to handle upstream if needed
            }
            catch (Exception ex)
            {
                Console.WriteLine($"General Error: {ex.Message}");
                throw; // Re-throw to handle upstream if needed
            }
        }

        public async Task ConnectAsync()
        {
            try
            {
                await _dbConnection.OpenAsync();
                Console.WriteLine("Connection opened successfully.");
            }
            catch (MySqlException ex)
            {
                Console.WriteLine($"MySQL Error: {ex.Message}");
                throw; // Re-throw to handle upstream if needed
            }
            catch (Exception ex)
            {
                Console.WriteLine($"General Error: {ex.Message}");
                throw; // Re-throw to handle upstream if needed
            }
        }

        public void Disconnect()
        {
            try
            {
                if (_dbConnection != null && _dbConnection.State != ConnectionState.Closed)
                {
                    _dbConnection.Close();
                    Console.WriteLine("Connection closed successfully.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error closing connection: {ex.Message}");
                throw; // Re-throw to handle upstream if needed
            }
        }

        public void AssociateItemToThisConnector(DbCommand dbCommand)
        {
            dbCommand.Connection = (MySqlConnection)_dbConnection;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool itIsSafeToFreeManagedObjects)
        {
            if (!itIsSafeToFreeManagedObjects) return;
            try
            {
                if (_dbConnection != null && _dbConnection.State != ConnectionState.Closed)
                {
                    _dbConnection.Close();
                }
                _dbConnection?.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error disposing connection: {ex.Message}");
                throw; // Re-throw to handle upstream if needed
            }
        }
    }
}