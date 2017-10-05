using System;
using NUnit.Framework;
using NUnit.Framework.Interfaces;

namespace Hangfire.Elasticsearch.Tests.TestInfrastructure
{
    public class ElasticOnlineAttribute : Attribute, ITestAction
    {
        private ElasticSearchContainer _elasticSearchContainer;

        public void BeforeTest(ITest test)
        {
            _elasticSearchContainer = ElasticSearchContainer.StartNewFromArchive(TestResources.elasticsearch_5_6_2);
        }

        public void AfterTest(ITest test)
        {
            _elasticSearchContainer.Dispose();
        }

        public ActionTargets Targets => ActionTargets.Test;
    }
}