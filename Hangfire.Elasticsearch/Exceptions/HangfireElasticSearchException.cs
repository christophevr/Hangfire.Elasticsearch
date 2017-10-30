using System;
using Nest;

namespace Hangfire.Elasticsearch.Exceptions
{
    public class HangfireElasticSearchException : Exception
    {
        public IResponse Response { get; }

        public HangfireElasticSearchException()
        {
        }

        public HangfireElasticSearchException(IResponse response)
        {
            Response = response;
        }

        public HangfireElasticSearchException(string message) : base(message)
        {
        }

        public HangfireElasticSearchException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}