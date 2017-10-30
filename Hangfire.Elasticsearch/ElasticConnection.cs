using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Elasticsearch.Extensions;
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

            var serverGetResponse = _elasticClient.Get<Model.Server>(serverId).ThrowIfInvalid();

            var server = serverGetResponse.Source ?? Model.Server.Create(serverId);
            server.LastHeartBeat = DateTime.UtcNow;
            server.WorkerCount = context.WorkerCount;
            server.Queues = context.Queues;

            _elasticClient.Index(server).ThrowIfInvalid();
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            _elasticClient.Delete<Model.Server>(serverId).ThrowIfInvalid();
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            var serverGetResponse = _elasticClient.Get<Model.Server>(serverId).ThrowIfInvalid();
            if (!serverGetResponse.Found)
                return;

            var server = serverGetResponse.Source;
            server.LastHeartBeat = DateTime.UtcNow;

            _elasticClient.Index(server).ThrowIfInvalid();
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));

            var timeOutAt = DateTime.UtcNow.Add(timeOut.Negate());
            var getTimedOutServerIdsReponse = _elasticClient.ScrollingSearch<Model.Server>(descr => descr
                .StoredFields(sf => sf.Fields(new string[0]))
                .Query(query => query
                    .DateRange(c => c
                        .Field(field => field.LastHeartBeat)
                        .LessThan(DateMath.Anchored(timeOutAt)))));

            var serverIds = getTimedOutServerIdsReponse.Select(x => x.Id);
            var bulkResponses = _elasticClient.BatchedBulk(serverIds, 
                (descr, serverId) => descr.Delete<object>(desc => desc.Id(serverId).Type<Model.Server>()));

            return bulkResponses.SelectMany(response => response.Items).Count();
        }

        public class Set
        {
            public string Id { get; set; }
            public SetValue[] SetValues { get; set; }
        }

        public class SetValue
        {
            public string Value { get; set; }
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            var setsResponse = _elasticClient.Get<Set>(key).ThrowIfInvalid();
            var values = setsResponse.Source.SetValues.Select(setValue => setValue.Value);
            return new HashSet<string>(values);
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