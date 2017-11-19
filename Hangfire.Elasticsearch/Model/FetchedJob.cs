using System;
using Hangfire.Storage;
using Nest;

namespace Hangfire.Elasticsearch.Model
{
    public class FetchedJob : IFetchedJob
    {
        private readonly IElasticClient _elasticClient;
        public string JobId { get; }

        public FetchedJob(JobDataDto jobData, IElasticClient elasticClient)
        {
            if (jobData == null)
                throw new ArgumentNullException(nameof(jobData));

            JobId = jobData.Id;
            _elasticClient = elasticClient;
        }

        public void RemoveFromQueue()
        {
            throw new System.NotImplementedException();
        }

        public void Requeue()
        {
            throw new System.NotImplementedException();
        }

        public void Dispose()
        {
            throw new System.NotImplementedException();
        }
    }
}