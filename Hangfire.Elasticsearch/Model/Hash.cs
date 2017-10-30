using System.Collections.Generic;

namespace Hangfire.Elasticsearch.Model
{
    public class Hash
    {
        public string Id { get; set; }
        public Dictionary<string, string> Hashes { get; set; }
    }
}