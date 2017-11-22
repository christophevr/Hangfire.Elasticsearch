using System;
using System.Text;
using Hangfire.Elasticsearch.Extensions;
using Nest;
using NUnit.Framework;

namespace Hangfire.Elasticsearch.Tests.TestInfrastructure
{
    internal class ElasticClientFactory
    {
        public static string TestIndexName => TestContext.CurrentContext.Test.MethodName.ToLower();

        public static IElasticClient CreateClientForTest()
        {
            var connectionSettingsValues = new ConnectionSettings()
                .DefaultIndex(TestIndexName)
                .ThrowExceptions(false)
                .DisableDirectStreaming()
                .OnRequestCompleted(details => { Console.WriteLine($@"Request completed. Sent a {details.HttpMethod} to {details.Uri} -- Request: {Encoding.UTF8.GetString(details.RequestBodyInBytes ?? new byte[0])} - Response: {Encoding.UTF8.GetString(details.ResponseBodyInBytes ?? new byte[0])}"); });

            var elasticClient = new ElasticClient(connectionSettingsValues);
            elasticClient.CreateIndex(TestIndexName).ThrowIfInvalid();

            return elasticClient;
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