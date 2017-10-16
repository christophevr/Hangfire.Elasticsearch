using System;
using Nest;

namespace Hangfire.Elasticsearch.Extensions
{
    public class HangfireElasticSearchException : Exception
    {
        public IResponse Response { get; }

        public HangfireElasticSearchException(string message, IResponse response)
            : base(message)
        {
            Response = response;
        }
    }
}