using System;
using Elasticsearch.Net;
using FluentAssertions;
using Hangfire.Elasticsearch.Exceptions;
using Hangfire.Elasticsearch.Tests.TestInfrastructure;
using Nest;
using NUnit.Framework;

namespace Hangfire.Elasticsearch.Tests
{
    [TestFixture]
    public class ElasticConnectionIntegrationTestElasticOffline
    {
        private IElasticClient _elasticClient;
        private ElasticConnection _elasticConnection;

        [SetUp]
        public void SetUp()
        {
            _elasticClient = ElasticClientFactory.CreateOfflineClient();
            _elasticConnection = new ElasticConnection(_elasticClient);
        }

        [Test]
        public void AnnounceServer_Throws()
        {
            // GIVEN
            const string serverId = "server-001";
            var serverContext = new Server.ServerContext
            {
                Queues = new[] { "queue 1", "queue 2" },
                WorkerCount = 32
            };

            // WHEN THEN
            Assert.Throws<HangfireElasticSearchException>(() => _elasticConnection.AnnounceServer(serverId, serverContext));
        }
        
        [Test]
        public void RemoveServer_Throws()
        {
            // GIVEN
            const string serverId = "server-001";

            // WHEN THEN
            Assert.Throws<HangfireElasticSearchException>(() => _elasticConnection.RemoveServer(serverId));
        }

        [Test]
        public void Heartbeat_Throws()
        {
            // GIVEN
            const string serverId = "server-001";

            // WHEN THEN
            Assert.Throws<HangfireElasticSearchException>(() => _elasticConnection.Heartbeat(serverId));
        }

        [Test]
        public void RemoveTimedOutServers_Throws()
        {
            // GIVEN WHEN THEN
            Assert.Throws<HangfireElasticSearchException>(() => _elasticConnection.RemoveTimedOutServers(TimeSpan.FromSeconds(1)));
        }
    }
}