using System;
using Hangfire.Storage;
using Nest;

namespace Hangfire.Elasticsearch
{
    public class ElasticStorage : JobStorage
    {
        private readonly HangfireElasticSettings _settings;

        public ElasticStorage(HangfireElasticSettings settings)
        {
            _settings = settings;
        }

        public override IMonitoringApi GetMonitoringApi()
        {
            throw new NotImplementedException();
        }

        public override IStorageConnection GetConnection()
        {
            var elasticClient = new ElasticClient();
            return new ElasticConnection(elasticClient, _settings);
        }
    }
}