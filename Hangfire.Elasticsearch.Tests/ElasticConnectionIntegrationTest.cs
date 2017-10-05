using System;
using Elasticsearch.Net;
using FluentAssertions;
using Hangfire.Elasticsearch.Tests.TestInfrastructure;
using Nest;
using NUnit.Framework;

namespace Hangfire.Elasticsearch.Tests
{
    [TestFixture]
    public class ElasticConnectionIntegrationTest
    {
        private IElasticClient _elasticClient;
        private ElasticConnection _elasticConnection;

        [SetUp]
        public void SetUp()
        {
            _elasticClient = new ElasticClient(new ConnectionSettings().DefaultIndex("hangfire-elasticsearch").ThrowExceptions());
            _elasticConnection = new ElasticConnection(_elasticClient);
        }

        [Test, ElasticOnline]
        public void AnnounceServer_WhenServerNotExists_SavesServer()
        {
            // GIVEN
            const string serverId = "server-001";
            var serverContext = new Server.ServerContext
            {
                Queues = new[] { "queue 1", "queue 2" },
                WorkerCount = 32
            };

            // WHEN
            _elasticConnection.AnnounceServer(serverId, serverContext);

            // THEN
            var serverResponse = _elasticClient.Get<Model.Server>(serverId);
            serverResponse.Found.Should().BeTrue();

            var server = serverResponse.Source;
            server.Queues.ShouldBeEquivalentTo(serverContext.Queues);
            server.WorkerCount.Should().Be(serverContext.WorkerCount);
        }

        [Test]
        public void AnnounceServer_WithElasticOffline_Throws()
        {
            // GIVEN
            const string serverId = "server-001";
            var serverContext = new Server.ServerContext
            {
                Queues = new[] { "queue 1", "queue 2" },
                WorkerCount = 32
            };

            // WHEN THEN
            Assert.Throws<ElasticsearchClientException>(() => _elasticConnection.AnnounceServer(serverId, serverContext));
        }

        [Test, ElasticOnline]
        public void AnnounceServer_WhenServerExists_UpdatesServer()
        {
            // GIVEN
            const string serverId = "server-001";
            _elasticClient.Index(new Model.Server {Id = serverId}, descr => descr.Refresh(Refresh.True));

            // WHEN
            var serverContext = new Server.ServerContext
            {
                Queues = new[] { "queue 1", "queue 2" },
                WorkerCount = 32
            };
            _elasticConnection.AnnounceServer(serverId, serverContext);

            // THEN
            var serverResponse = _elasticClient.Get<Model.Server>(serverId);
            serverResponse.Found.Should().BeTrue();

            var server = serverResponse.Source;
            server.Queues.ShouldBeEquivalentTo(serverContext.Queues);
            server.WorkerCount.Should().Be(serverContext.WorkerCount);
        }

        [Test, ElasticOnline]
        public void RemoveServer_WithExistingServer_RemovesServer()
        {
            // GIVEN
            const string serverId = "server-001";
            _elasticClient.Index(new Model.Server { Id = serverId }, descr => descr.Refresh(Refresh.True));

            // WHEN
            _elasticConnection.RemoveServer(serverId);

            // THEN
            var getServerResponse = _elasticClient.Get<Model.Server>(serverId);
            getServerResponse.Found.Should().BeFalse();
        }

        [Test, ElasticOnline]
        public void RemoveServer_WithoutExistingServer_DoesNothing()
        {
            // GIVEN
            const string serverId = "server-001";

            // WHEN
            _elasticConnection.RemoveServer(serverId);

            // THEN
            var getServerResponse = _elasticClient.Get<Model.Server>(serverId);
            getServerResponse.Found.Should().BeFalse();
        }

        [Test]
        public void RemoveServer_WithElasticOffline_Throws()
        {
            // GIVEN
            const string serverId = "server-001";

            // WHEN THEN
            Assert.Throws<ElasticsearchClientException>(() => _elasticConnection.RemoveServer(serverId));
        }

        [Test, ElasticOnline]
        public void Heartbeat_WithExistingServer_UpdatesHeartbeat()
        {
            // GIVEN
            const string serverId = "server-001";
            var server = new Model.Server { Id = serverId, LastHeartBeat = new DateTime(2017, 10, 1) };
            _elasticClient.Index(server, descr => descr.Refresh(Refresh.True));

            // WHEN
            _elasticConnection.Heartbeat(serverId);

            // THEN
            var getServerResponse = _elasticClient.Get<Model.Server>(serverId);
            getServerResponse.Found.Should().BeTrue();
            getServerResponse.Source.LastHeartBeat.Should().NotBe(server.LastHeartBeat);
        }

        [Test, ElasticOnline]
        public void Heartbeat_WithNoExistingServer_DoesNothing()
        {
            // GIVEN
            const string serverId = "server-001";

            // WHEN
            _elasticConnection.Heartbeat(serverId);

            // THEN
            var getServerResponse = _elasticClient.Count<Model.Server>(descr => descr.Query(q => q.MatchAll()));
            getServerResponse.Count.Should().Be(0);
        }

        [Test]
        public void Heartbeat_WithElasticOffline_Throws()
        {
            // GIVEN
            const string serverId = "server-001";

            // WHEN THEN
            Assert.Throws<ElasticsearchClientException>(() => _elasticConnection.Heartbeat(serverId));
        }
    }
}