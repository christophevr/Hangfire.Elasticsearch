using System;
using System.Text;
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
                .ThrowExceptions(false)
                .DisableDirectStreaming()
                .OnRequestCompleted(details => { Console.WriteLine($@"Request completed. Sent a {details.HttpMethod} to {details.Uri} -- Request: {Encoding.UTF8.GetString(details.RequestBodyInBytes ?? new byte[0])} - Response: {Encoding.UTF8.GetString(details.ResponseBodyInBytes ?? new byte[0])}"); });
            return new ElasticClient(connectionSettingsValues);
        }

        public static IElasticClient CreateOfflineClient()
        {
            var offlineUri = new Uri("http://localhost:9201/");
            var connectionSettingsValues = new ConnectionSettings(offlineUri)
                .DefaultIndex("default")
                .ThrowExceptions(false);
            return new ElasticClient(connectionSettingsValues);
        }
    }
}