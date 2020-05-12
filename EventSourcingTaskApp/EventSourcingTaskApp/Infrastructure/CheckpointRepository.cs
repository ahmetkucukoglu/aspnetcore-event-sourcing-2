namespace EventSourcingTaskApp.Infrastructure
{
    using Couchbase;
    using Couchbase.Core;
    using Couchbase.Extensions.DependencyInjection;
    using EventStore.ClientAPI;
    using System.Threading.Tasks;

    public class CheckpointRepository
    {
        private readonly IBucket _bucket;

        public CheckpointRepository(IBucketProvider bucketProvider)
        {
            _bucket = bucketProvider.GetBucket("checkpoints");
        }

        public async Task<Position?> GetAsync(string key)
        {
            var result = await _bucket.GetAsync<CheckpointDocument>(key);

            if (result.Value == null)
                return null;

            return result.Value.Position;
        }

        public async Task<bool> SaveAsync(string key, Position position)
        {
            var doc = new Document<CheckpointDocument>
            {
                Id = key,
                Content = new CheckpointDocument
                {
                    Key = key,
                    Position = position
                }
            };

            var result = await _bucket.UpsertAsync(doc);

            return result.Success;
        }
    }
}
