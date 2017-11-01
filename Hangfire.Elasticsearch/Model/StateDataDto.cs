using System.Collections.Generic;
using Hangfire.Storage;

namespace Hangfire.Elasticsearch.Model
{
    public class StateDataDto
    {
        public string Name { get; set; }
        public string Reason { get; set; }
        public Dictionary<string, string> Data { get; set; }

        public StateData ToStateData()
        {
            return new StateData
            {
                Name = Name,
                Reason = Reason,
                Data = Data
            };
        }
    }
}