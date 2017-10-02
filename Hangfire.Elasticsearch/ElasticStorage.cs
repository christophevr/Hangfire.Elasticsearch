using System;
using Hangfire.Storage;

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
            return new ElasticConnection();
        }
    }
}