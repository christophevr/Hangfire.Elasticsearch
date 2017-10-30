namespace Hangfire.Elasticsearch.Model
{
    public class Set
    {
        public string Id { get; set; }
        public SetValue[] SetValues { get; set; }
    }
}