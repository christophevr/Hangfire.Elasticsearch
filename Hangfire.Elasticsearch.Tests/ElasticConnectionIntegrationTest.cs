using FluentAssertions;
using Hangfire.Elasticsearch.Tests.TestInfrastructure;
using Nest;
using NUnit.Framework;

namespace Hangfire.Elasticsearch.Tests
{
    public class ElasticConnectionIntegrationTest
    {
        private ElasticSearchContainer _elasticSearchContainer;
        private IElasticClient _elasticClient;
        private ElasticConnection _elasticConnection;

        [SetUp]
        public void SetUp()
        {
            // TODO move common testing logic to central location
            _elasticSearchContainer = ElasticSearchContainer.StartNewFromArchive(TestResources.elasticsearch_5_6_2);
            _elasticClient = new ElasticClient(new ConnectionSettings().DefaultIndex("hangfire-elasticsearch"));
            _elasticConnection = new ElasticConnection(_elasticClient);
        }

        [TearDown]
        public void TearDown()
        {
            _elasticSearchContainer.Dispose();
        }

        [Test]
        public void AnnounceServer_PersistsServerObject()
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
            _elasticClient.Refresh(Indices.All);
            var serverResponse = _elasticClient.Get<Model.Server>(serverId);
            serverResponse.Found.Should().BeTrue();

            var server = serverResponse.Source;
            server.Queues.ShouldBeEquivalentTo(serverContext.Queues);
            server.WorkerCount.Should().Be(serverContext.WorkerCount);
        }
    }
}