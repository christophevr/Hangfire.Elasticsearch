using System;
using Hangfire.Common;
using Hangfire.Storage;

namespace Hangfire.Elasticsearch.Model
{
    public class JobDataDto
    {
        public string Id { get; set; }
        public string StateName { get; set; }
        public DateTime CreatedAt { get; set; }
        public StateDataDto StateData { get; set; }
        public InvocationDataDto InvocationDataDto { get; set; }

        public JobData ToJobData()
        {
            Job job = null;
            JobLoadException loadException = null;

            try
            {
                var invocationData = InvocationDataDto.ToInvocationData();
                job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                LoadException = loadException,
                State = StateName,
                CreatedAt = CreatedAt
            };
        }
    }
}