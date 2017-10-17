using System.Linq;
using FluentAssertions;
using Hangfire.Elasticsearch.Extensions;
using NUnit.Framework;

namespace Hangfire.Elasticsearch.Tests.Extensions
{
    [TestFixture]
    public class EnumerableExtensionsTest
    {
        [Test]
        public void Batch_ReturnsExpectedBatches()
        {
            // GIVEN
            var items = Enumerable.Range(0, 10);

            // WHEN
            var batches = items.Batch(2).ToList();

            // THEN
            batches.Should().HaveCount(5);
            foreach (var batch in batches)
            {
                batch.Should().HaveCount(2);
            }
        }
    }
}