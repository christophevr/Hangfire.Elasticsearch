using System;
using System.Collections.Generic;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using Nest;

namespace Hangfire.Elasticsearch
{
    public class ElasticConnection : JobStorageConnection
    {
        private readonly IElasticClient _elasticClient;

        public ElasticConnection(IElasticClient elasticClient)
        {
            _elasticClient = elasticClient;
        }
        
        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            throw new NotImplementedException();
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            throw new NotImplementedException();
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            throw new NotImplementedException();
        }

        public override string GetJobParameter(string id, string name)
        {
            throw new NotImplementedException();
        }

        public override JobData GetJobData(string jobId)
        {
            throw new NotImplementedException();
        }

        public override StateData GetStateData(string jobId)
        {
            throw new NotImplementedException();
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            var serverGetResponse = _elasticClient.Get<Model.Server>(serverId);
            var server = serverGetResponse.Source ?? Model.Server.Create(serverId);

            server.LastHeartBeat = DateTime.UtcNow;
            server.WorkerCount = context.WorkerCount;
            server.Queues = context.Queues;

            _elasticClient.Index(server);
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            _elasticClient.Delete<Model.Server>(serverId);
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            var serverGetResponse = _elasticClient.Get<Model.Server>(serverId);
            if (!serverGetResponse.Found)
                return;

            var server = serverGetResponse.Source;
            server.LastHeartBeat = DateTime.UtcNow;

            _elasticClient.Index(server);
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            throw new NotImplementedException();
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            throw new NotImplementedException();
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            throw new NotImplementedException();
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            throw new NotImplementedException();
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            throw new NotImplementedException();
        }
    }
}