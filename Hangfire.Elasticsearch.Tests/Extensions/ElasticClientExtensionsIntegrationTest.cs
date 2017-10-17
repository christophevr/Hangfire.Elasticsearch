using System.Diagnostics.CodeAnalysis;
using System.Linq;
using FluentAssertions;
using Hangfire.Elasticsearch.Extensions;
using Hangfire.Elasticsearch.Tests.TestInfrastructure;
using Nest;
using NUnit.Framework;

namespace Hangfire.Elasticsearch.Tests.Extensions
{
    [TestFixture]
    public class ElasticClientExtensionsIntegrationTest
    {
        private IElasticClient _elasticClient;

        [SetUp]
        public void SetUp()
        {
            _elasticClient = ElasticClientFactory.CreateClientForTest();
        }

        [Test]
        public void ScrollingSearch_ReturnsExpectedDocuments()
        {
            // GIVEN
            const int documentCount = 5000;
            var demoDocs = Enumerable.Range(0, documentCount).Select(i => new DemoDoc {Id = $"doc-{i}"}).ToList();
            _elasticClient.IndexMany(demoDocs);
            _elasticClient.Refresh(Indices.All);
            _elasticClient.Count<DemoDoc>().Count.Should().Be(documentCount);

            // WHEN
            var actualDocs = _elasticClient.ScrollingSearch<DemoDoc>(descr => descr.Query(q => q.MatchAll())).ToList();

            // THEN
            actualDocs.Should().HaveCount(documentCount);
        }

        [Test]
        public void BatchedBulk_WithDeleteOperation_DeletesExpectedDocuments()
        {
            // GIVEN
            const int documentCount = 5000;
            var demoDocs = Enumerable.Range(0, documentCount).Select(i => new DemoDoc { Id = $"doc-{i}" }).ToList();
            _elasticClient.IndexMany(demoDocs);
            _elasticClient.Refresh(Indices.All);
            _elasticClient.Count<DemoDoc>().Count.Should().Be(documentCount);

            // WHEN
            var bulkResponses = _elasticClient.BatchedBulk(demoDocs,
                (descr, demoDoc) => descr.Delete<DemoDoc>(delete => delete.Id(demoDoc.Id).Type<DemoDoc>()))
                .ToList();

            // THEN
            _elasticClient.Refresh(Indices.All);
            _elasticClient.Count<DemoDoc>().Count.Should().Be(0);
            bulkResponses.SelectMany(response => response.Items).Should().HaveCount(documentCount);
        }


        [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
        private class DemoDoc
        {
            public string Id { get; set; }
        }
    }
}