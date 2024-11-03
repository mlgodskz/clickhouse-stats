using ClickHouse.Client.ADO;
using System.Diagnostics;

class Program
{
    static async Task Main()
    {
        var connectionString = "Host=localhost;Port=8123;Username=default;Password=5555;Database=mining";
        
        using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        // Прогрев соединения
        using (var warmupCommand = connection.CreateCommand())
        {
            warmupCommand.CommandText = "SELECT 1";
            await warmupCommand.ExecuteScalarAsync();
        }

        var queries = new[]
        {
            // Проверка содержимого MV
            @"SELECT 
                period_start,
                channel_id,
                share_count,
                total_hashes,
                min_timestamp,
                max_timestamp
            FROM mv_channel_stats
            ORDER BY period_start, channel_id
            LIMIT 10",

            // Общее количество записей
            "SELECT sum(share_count) FROM mv_channel_stats",

            // Статистика по каналам за весь период
            @"SELECT 
                channel_id,
                sum(share_count) as total_shares,
                sum(total_hashes) / (max(max_timestamp) - min(min_timestamp)) as hashrate
            FROM mv_channel_stats
            GROUP BY channel_id
            ORDER BY channel_id"
        };

        foreach (var query in queries)
        {
            var sw = Stopwatch.StartNew();
            
            using var command = connection.CreateCommand();
            command.CommandText = query;

            if (query.Contains("LIMIT 10"))
            {
                Console.WriteLine("\nПример содержимого mv_channel_stats (первые 10 записей):");
                Console.WriteLine("------------------------------------------------------------------------------------------------");
                Console.WriteLine("period_start          | channel_id | share_count | total_hashes | min_timestamp         | max_timestamp");
                Console.WriteLine("------------------------------------------------------------------------------------------------");
                
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    Console.WriteLine($"{reader["period_start"],-20} | {reader["channel_id"],-10} | {reader["share_count"],-11} | {reader["total_hashes"],-12:F0} | {reader["min_timestamp"],-20} | {reader["max_timestamp"]}");
                }
                Console.WriteLine("------------------------------------------------------------------------------------------------");
            }
            else if (query.Contains("GROUP BY"))
            {
                Console.WriteLine("\nСтатистика по channel_id:");
                Console.WriteLine("-------------------------------------------------------------------------");
                Console.WriteLine("channel_id | количество шар | средний хэшрейт (H/s)");
                Console.WriteLine("-------------------------------------------------------------------------");
                
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    Console.WriteLine($"{reader["channel_id"],-10} | {reader["total_shares"],-14} | {reader["hashrate"]:F2}");
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