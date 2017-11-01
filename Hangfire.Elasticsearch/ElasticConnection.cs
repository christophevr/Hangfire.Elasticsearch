﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Elasticsearch.Extensions;
using Hangfire.Elasticsearch.Model;
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
            if (string.IsNullOrEmpty(id))
                throw new ArgumentNullException(nameof(id));
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(nameof(name));

            var jobDataResponse = _elasticClient.Get<JobDataDto>(id).ThrowIfInvalid();
            if (!jobDataResponse.Found)
                return null;

            var jobDataDto = jobDataResponse.Source;
            if (jobDataDto.JobParameters.TryGetValue(name, out var parameterValue))
                return parameterValue;

            return null;
        }

        public override JobData GetJobData(string jobId)
        {
            if (string.IsNullOrEmpty(jobId))
                throw new ArgumentNullException(nameof(jobId));

            var jobDataResponse = _elasticClient.Get<JobDataDto>(jobId).ThrowIfInvalid();
            if (!jobDataResponse.Found)
                return null;

            var jobDataDto = jobDataResponse.Source;
            return jobDataDto.ToJobData();
        }

        public override StateData GetStateData(string jobId)
        {
            if (string.IsNullOrEmpty(jobId))
                throw new ArgumentNullException(nameof(jobId));

            var jobDataResponse = _elasticClient.Get<JobDataDto>(jobId).ThrowIfInvalid();
            if (!jobDataResponse.Found)
                return null;

            var jobData = jobDataResponse.Source;
            return jobData.StateDataDto?.ToStateData();
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

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            var setsResponse = _elasticClient.Get<Set>(key).ThrowIfInvalid();
            var set = setsResponse.Source;
            var values = set.SetValues.Select(setValue => setValue.Value);
            return new HashSet<string>(values);
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
            if (fromScore > toScore)
                throw new ArgumentException($"'{nameof(fromScore)}' cannot have a higher value than '{nameof(toScore)}'");

            var setsResponse = _elasticClient.Get<Set>(key).ThrowIfInvalid();
            if (!setsResponse.Found)
                return null;

            var set = setsResponse.Source;
            var matchingValues = set.SetValues.Where(sv => sv.Score >= fromScore && sv.Score <= toScore);
            return matchingValues.OrderBy(sv => sv.Score).FirstOrDefault()?.Value;
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null)
                throw new ArgumentNullException(nameof(keyValuePairs));

            var hashResponse = _elasticClient.Get<Hash>(key).ThrowIfInvalid();
            if (hashResponse.Found)
            {
                var hash = hashResponse.Source;
                foreach (var keyValuePair in keyValuePairs)
                    hash.Hashes[keyValuePair.Key] = keyValuePair.Value;

                _elasticClient.Index(hash, descr => descr.Version(hashResponse.Version)).ThrowIfInvalid();
            }
            else
            {
                var hash = new Hash
                {
                    Id = key,
                    Hashes = new Dictionary<string, string>()
                };

                foreach (var keyValuePair in keyValuePairs)
                    hash.Hashes[keyValuePair.Key] = keyValuePair.Value;

                _elasticClient.Index(hash).ThrowIfInvalid();
            }
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key));

            var hashResponse = _elasticClient.Get<Hash>(key).ThrowIfInvalid();
            if (!hashResponse.Found)
                return null;

            return hashResponse.Source.Hashes;
        }
    }
}