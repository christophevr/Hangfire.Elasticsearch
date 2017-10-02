using System;

namespace Hangfire.Elasticsearch.Model
{
    public class Server
    {
        public string Id { get; set; }
        public DateTime LastHeartBeat { get; set; }
        public int WorkerCount { get; set; }
        public DateTime StartedAt { get; set; }
        public string[] Queues { get; set; }

        public static Server Create(string serverId)
        {
            return new Server
            {
                Id = serverId,
                StartedAt = DateTime.UtcNow
            };
        }
    }
}