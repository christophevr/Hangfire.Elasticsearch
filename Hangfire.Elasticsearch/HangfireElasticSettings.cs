using System;

namespace Hangfire.Elasticsearch
{
    public class HangfireElasticSettings
    {
        public HangfireElasticSettings()
        {
            FetchNextJobTimeout = TimeSpan.FromSeconds(30);
        }

        public TimeSpan FetchNextJobTimeout { get; set; }
    }
}