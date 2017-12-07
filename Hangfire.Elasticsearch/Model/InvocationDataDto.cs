using Hangfire.Storage;

namespace Hangfire.Elasticsearch.Model
{
    public class InvocationDataDto
    {
        public string Type { get; set; }
        public string Method { get; set; }
        public string ParameterTypes { get; set; }
        public string Arguments { get; set; }

        public InvocationData ToInvocationData()
        {
            return new InvocationData(Type, Method, ParameterTypes, Arguments);
        }

        public static InvocationDataDto Create(InvocationData invocationData)
        {
            return new InvocationDataDto
            {
                Type = invocationData.Type,
                Arguments = invocationData.Arguments,
                Method = invocationData.Method,
                ParameterTypes = invocationData.ParameterTypes
            };
        }
    }
}