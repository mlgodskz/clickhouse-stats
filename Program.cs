using ClickHouse.Client.ADO;
using System.Diagnostics;

class Program
{
    static async Task Main()
    {
        var connectionString = "Host=localhost;Port=8123;Username=default;Password=5555;Database=mining";
        
        using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        // Запросы
        var queries = new[]
        {
            "SELECT COUNT(*) FROM fake_share_logs",
            "SELECT channel_id, COUNT(*) as count FROM fake_share_logs GROUP BY channel_id ORDER BY channel_id"
        };

        foreach (var query in queries)
        {
            var sw = Stopwatch.StartNew();
            
            using var command = connection.CreateCommand();
            command.CommandText = query;

            if (query.Contains("GROUP BY"))
            {
                Console.WriteLine("\nКоличество записей по channel_id:");
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    Console.WriteLine($"channel_id: {reader["channel_id"]}, количество: {reader["count"]}");
                }
            }
            else
            {
                var result = await command.ExecuteScalarAsync();
                Console.WriteLine($"\nОбщее количество записей: {result}");
            }

            sw.Stop();
            Console.WriteLine($"Время выполнения запроса: {sw.Elapsed.TotalMilliseconds:F3} мс");
        }
    }
}