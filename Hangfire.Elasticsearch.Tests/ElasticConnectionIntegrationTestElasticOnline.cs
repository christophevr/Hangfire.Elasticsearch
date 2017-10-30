using System;
using System.Linq;
using Elasticsearch.Net;
using FluentAssertions;
using Hangfire.Elasticsearch.Model;
using Hangfire.Elasticsearch.Tests.TestInfrastructure;
using Nest;
using NUnit.Framework;

namespace Hangfire.Elasticsearch.Tests
{
    [TestFixture]
    public class ElasticConnectionIntegrationTestElasticOnline
    {
        private IElasticClient _elasticClient;
        private ElasticConnection _elasticConnection;

        [SetUp]
        public void SetUp()
        {
            _elasticClient = ElasticClientFactory.CreateClientForTest();
            _elasticConnection = new ElasticConnection(_elasticClient);
        }

        [Test]
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
        public void AnnounceServer_WhenServerExists_UpdatesServer()
        {
            // GIVEN
            const string serverId = "server-001";
            _elasticClient.Index(new Model.Server { Id = serverId }, descr => descr.Refresh(Refresh.True));

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

        [Test]
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

        [Test]
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

        [Test]
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
        public void RemoveTimedOutServers_RemovesTimedOutServer()
        {
            // GIVEN
            const string serverId = "server-001";
            var server = new Model.Server { Id = serverId, LastHeartBeat = new DateTime(2000, 1, 1) };
            _elasticClient.Index(server, descr => descr.Refresh(Refresh.True));

            // WHEN
            var removedServersCount = _elasticConnection.RemoveTimedOutServers(TimeSpan.FromSeconds(1));

            // THEN
            removedServersCount.Should().Be(1);
            var getServerResponse = _elasticClient.Get<Model.Server>(serverId);
            getServerResponse.Found.Should().BeFalse();
        }

        [Test]
        public void RemoveTimedOutServers_WithLotsOfTimedOutServers_RemovesTimedOutServer()
        {
            // GIVEN
            const int elasticMaxResponseDocumentCount = 5000;
            var servers = Enumerable.Range(0, elasticMaxResponseDocumentCount + 1)
                .Select(i => new Model.Server { Id = $"server-{i}", LastHeartBeat = new DateTime(2000, 1, 1) });
            _elasticClient.IndexMany(servers);
            _elasticClient.Refresh(Indices.All);

            // WHEN
            var removedServersCount = _elasticConnection.RemoveTimedOutServers(TimeSpan.FromSeconds(1));

            // THEN
            removedServersCount.Should().Be(elasticMaxResponseDocumentCount + 1);
            _elasticClient.Refresh(Indices.All);
            var getServerResponse = _elasticClient.Count<Model.Server>(descr => descr.Query(q => q.MatchAll()));
            getServerResponse.Count.Should().Be(0);
        }

        [Test]
        public void RemoveTimedOutServers_WithNoTimedOutServers_DoesNothing()
        {
            // GIVEN
            const string serverId = "server-001";
            var server = new Model.Server { Id = serverId, LastHeartBeat = DateTime.UtcNow };
            _elasticClient.Index(server, descr => descr.Refresh(Refresh.True));

            // WHEN
            var removedServersCount = _elasticConnection.RemoveTimedOutServers(TimeSpan.FromDays(1));

            // THEN
            removedServersCount.Should().Be(0);
            var getServerResponse = _elasticClient.Get<Model.Server>(serverId);
            getServerResponse.Found.Should().BeTrue();
            getServerResponse.Source.ShouldBeEquivalentTo(server);
        }

        [Test]
        public void RemoveTimedOutServers_WithNegativeTimeout_Throws()
        {
            // GIVEN
            var timeout = TimeSpan.FromSeconds(-1);

            // WHEN THEN
            Assert.Throws<ArgumentException>(() => _elasticConnection.RemoveTimedOutServers(timeout));
        }

        [Test]
        public void GetAllItemsFromSet_WithNullKey_Throws()
        {
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.GetAllItemsFromSet(null));
        }

        [Test]
        public void GetAllItemsFromSet_ReturnsExpectedItems()
        {
            // GIVEN
            var sets = new[]
            {
                new Set
                {
                    Id = "key-1",
                    SetValues = new[]
                    {
                        new SetValue {Value = "value-1"},
                        new SetValue {Value = "value-2"},
                    }
                },
                new Set
                {
                    Id = "key-2",
                    SetValues = new[]
                    {
                        new SetValue {Value = "value-3"}
                    }
                }
            };
            _elasticClient.IndexMany(sets);
            _elasticClient.Refresh(Indices.All);

            // WHEN
            var set = _elasticConnection.GetAllItemsFromSet("key-1");

            // THEN
            set.Count.Should().Be(2);
            set.ShouldAllBeEquivalentTo(new[] { "value-1", "value-2" });
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_WithNullKey_Throws()
        {
            // GIVEN
            const string key = null;
            const double fromScore = 10;
            const double toScore = 15;

            // WHEN THEN
            Assert.Throws<ArgumentNullException>(() => _elasticConnection.GetFirstByLowestScoreFromSet(key, fromScore, toScore));
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_WithFromScore_HigherThan_ToScore_Throws()
        {
            // GIVEN
            const string key = "key";
            const double fromScore = 15;
            const double toScore = 10;

            // WHEN THEN
            Assert.Throws<ArgumentException>(() => _elasticConnection.GetFirstByLowestScoreFromSet(key, fromScore, toScore));
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_ReturnsExpectedItem()
        {
            // GIVEN
            const string key = "key-1";
            var set = new Set
            {
                Id = key,
                SetValues = new[]
                {
                    new SetValue {Value = "value-1", Score = 10},
                    new SetValue {Value = "value-2", Score = 15},
                    new SetValue {Value = "value-3", Score = 20},
                }
            };
            _elasticClient.Index(set);
            _elasticClient.Refresh(Indices.All);

            // WHEN
            var value = _elasticConnection.GetFirstByLowestScoreFromSet(key, 13, 17);

            // THEN
            value.Should().NotBeNull();
            value.Should().Be("value-2");
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_WithNoMatchingSet_ReturnsNull()
        {
            // GIVEN
            const string key = "key-1";

            // WHEN
            var value = _elasticConnection.GetFirstByLowestScoreFromSet(key, 10, 15);

            // THEN
            value.Should().BeNull();
        }

        [Test]
        public void GetFirstByLowestScoreFromSet_WithNoMatchingScores_ReturnsNull()
        {
            // GIVEN
            const string key = "key-1";
            var set = new Set
            {
                Id = key,
                SetValues = new[]
                {
                    new SetValue {Value = "value-1", Score = 10},
                }
            };
            _elasticClient.Index(set);
            _elasticClient.Refresh(Indices.All);

            // WHEN
            var value = _elasticConnection.GetFirstByLowestScoreFromSet(key, 15, 20);

            // THEN
            value.Should().BeNull();
        }
    }
}