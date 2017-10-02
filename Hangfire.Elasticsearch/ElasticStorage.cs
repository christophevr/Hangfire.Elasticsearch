using System;
using Hangfire.Storage;
using Nest;

namespace Hangfire.Elasticsearch
{
    public class ElasticStorage : JobStorage
    {
        public override IMonitoringApi GetMonitoringApi()
        {
            throw new NotImplementedException();
        }

        public override IStorageConnection GetConnection()
        {
            var elasticClient = new ElasticClient();
            return new ElasticConnection(elasticClient);
        }
    }
}