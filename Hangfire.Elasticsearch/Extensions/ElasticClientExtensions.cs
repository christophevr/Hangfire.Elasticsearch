﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Nest;

namespace Hangfire.Elasticsearch.Extensions
{
    internal static class ElasticClientExtensions
    {
        public static IEnumerable<IHit<T>> ScrollingSearch<T>(this IElasticClient client, Func<SearchDescriptor<T>, ISearchRequest> searchDescriptor, int scrollTimeoutInSeconds = 60, int batchSize = 1000) 
            where T : class
        {
            var scrollTimeout = new Time(TimeSpan.FromSeconds(scrollTimeoutInSeconds));
            var response = client.Search<T>(descr =>
            {
                var scrollDescriptor = descr.Scroll(scrollTimeout).Size(batchSize);
                return searchDescriptor(scrollDescriptor);
            });

            if (!response.Documents.Any())
                yield break;

            do
            {
                foreach (var document in response.Hits)
                    yield return document;

                var scrollId = response.ScrollId;
                response = client.Scroll<T>(scrollTimeout, scrollId);
            } while (response.Documents.Any());
        }
    }
}