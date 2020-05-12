namespace EventSourcingTaskApp.Infrastructure
{
    using Couchbase;
    using Couchbase.Core;
    using Couchbase.Extensions.DependencyInjection;
    using EventSourcingTaskApp.Core;
    using EventSourcingTaskApp.Core.Events;
    using System;
    using System.Threading.Tasks;

    public class TaskRepository
    {
        private readonly IBucket _bucket;

        public TaskRepository(IBucketProvider bucketProvider)
        {
            _bucket = bucketProvider.GetBucket("tasks");
        }

        public void Save(object @event)
        {
            switch (@event)
            {
                case CreatedTask x: OnCreated(x); break;
                case AssignedTask x: OnAssigned(x); break;
                case MovedTask x: OnMoved(x); break;
                case CompletedTask x: OnCompleted(x); break;
            }
        }

        public async Task<TaskDocument> Get(Guid taskId)
        {
            var documentResult = await _bucket.GetDocumentAsync<TaskDocument>(taskId.ToString());

            return documentResult.Document.Content;
        }

        private async void OnCreated(CreatedTask @event)
        {
            var document = new Document<TaskDocument>
            {
                Id = @event.TaskId.ToString(),
                Content = new TaskDocument
                {
                    Id = @event.TaskId,
                    Title = @event.Title,
                    Section = BoardSections.Open
                }
            };

            await _bucket.InsertAsync(document);
        }

        private async void OnAssigned(AssignedTask @event)
        {
            await _bucket.MutateIn<TaskDocument>(@event.TaskId.ToString())
                .Replace("assignedTo", @event.AssignedTo)
                .ExecuteAsync();
        }

        private async void OnMoved(MovedTask @event)
        {
            await _bucket.MutateIn<TaskDocument>(@event.TaskId.ToString())
                .Replace("section", @event.Section)
                .ExecuteAsync();
        }

        private async void OnCompleted(CompletedTask @event)
        {
            await _bucket.MutateIn<TaskDocument>(@event.TaskId.ToString())
                .Replace("completedBy", @event.CompletedBy)
                .ExecuteAsync();
        }
    }
}
