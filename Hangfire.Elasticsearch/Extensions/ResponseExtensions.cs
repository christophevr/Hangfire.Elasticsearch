using Nest;

namespace Hangfire.Elasticsearch.Extensions
{
    public static class ResponseExtensions
    {
        public static TResponse ThrowIfInvalid<TResponse>(this TResponse response) where TResponse : IResponse
        {
            if (!response.IsValid)
                throw new Exceptions.HangfireElasticSearchException(response);

            return response;
        }
    }
}