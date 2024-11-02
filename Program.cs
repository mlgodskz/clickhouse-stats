using ClickHouse.Client.ADO;
using System.Diagnostics;

class Program
{
    static async Task Main()
    {
        var connectionString = "Host=localhost;Port=8123;Username=default;Password=5555;Database=mining";
        
        using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        var queries = new[]
        {
            "SELECT COUNT(*) FROM fake_share_logs",
            @"WITH channel_stats AS (
                SELECT 
                    channel_id,
                    COUNT(*) as share_count,
                    MIN(timestamp) as period_start,
                    MAX(timestamp) as period_end,
                    SUM(difficulty * pow(2, 32)) as total_hashes
                FROM fake_share_logs 
                GROUP BY channel_id
            )
            SELECT 
                channel_id,
                share_count,
                total_hashes / (toUnixTimestamp(period_end) - toUnixTimestamp(period_start)) as hashrate
            FROM channel_stats
            ORDER BY channel_id"
        };

        foreach (var query in queries)
        {
            var sw = Stopwatch.StartNew();
            
            using var command = connection.CreateCommand();
            command.CommandText = query;

            if (query.Contains("GROUP BY"))
            {
                Console.WriteLine("\nСтатистика по channel_id:");
                Console.WriteLine("-------------------------------------------------------------------------");
                Console.WriteLine("channel_id | количество шар | средний хэшрейт (H/s)");
                Console.WriteLine("-------------------------------------------------------------------------");
                
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    Console.WriteLine($"{reader["channel_id"],-10} | {reader["share_count"],-14} | {reader["hashrate"]:F2}");
                }
                Console.WriteLine("-------------------------------------------------------------------------");
            }
            else
            {
                var result = await command.ExecuteScalarAsync();
                Console.WriteLine($"\nОбщее количество записей: {result}");
            }

            sw.Stop();
            Console.WriteLine($"Время выполнения запроса: {sw.Elapsed.TotalSeconds:F6} sec");
        }
    }
}