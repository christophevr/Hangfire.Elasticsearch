using System;
using Nest;
using NUnit.Framework;

namespace Hangfire.Elasticsearch.Tests.TestInfrastructure
{
    internal class ElasticClientFactory
    {
        public static IElasticClient CreateClientForTest()
        {
            var testName = TestContext.CurrentContext.Test.MethodName.ToLower();
            var connectionSettingsValues = new ConnectionSettings()
                .DefaultIndex(testName)
                .ThrowExceptions();
            return new ElasticClient(connectionSettingsValues);
        }

        public static IElasticClient CreateOfflineClient()
        {
            var offlineUri = new Uri("http://localhost:9201/");
            var connectionSettingsValues = new ConnectionSettings(offlineUri)
                .DefaultIndex("default")
                .ThrowExceptions();
            return new ElasticClient(connectionSettingsValues);
        }
    }
}