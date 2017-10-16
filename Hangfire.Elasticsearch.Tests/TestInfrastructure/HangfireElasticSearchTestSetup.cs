using Hangfire.Elasticsearch.Tests.TestInfrastructure;
using NUnit.Framework;

[SetUpFixture]
// ReSharper disable once CheckNamespace
public class HangfireElasticSearchTestSetup
{
    private ElasticSearchContainer _elasticSearchContainer;

    [OneTimeSetUp]
    public void SetUp()
    {
        _elasticSearchContainer = ElasticSearchContainer.StartNewFromArchive(TestResources.elasticsearch_5_6_2);
        _elasticSearchContainer.WaitUntilElasticOperational();
    }

    [OneTimeTearDown]
    public void TearDown()
    {
        _elasticSearchContainer.Dispose();
    }
}