namespace EventSourcingTaskApp.HostedServices
{
    using EventSourcingTaskApp.Core.Events;
    using EventSourcingTaskApp.Infrastructure;
    using EventStore.ClientAPI;
    using Microsoft.Extensions.Hosting;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;

    public class TaskHostedService : IHostedService
    {
        private readonly IEventStoreConnection _eventStore;
        private readonly CheckpointRepository _checkpointRepository;
        private readonly TaskRepository _taskRepository;
        private readonly ILogger<TaskHostedService> _logger;

        private EventStoreAllCatchUpSubscription subscription;

        public TaskHostedService(IEventStoreConnection eventStore, CheckpointRepository checkpointRepository, TaskRepository taskRepository, ILogger<TaskHostedService> logger)
        {
            _eventStore = eventStore;
            _checkpointRepository = checkpointRepository;
            _taskRepository = taskRepository;
            _logger = logger;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var lastCheckpoint = await _checkpointRepository.GetAsync("tasks");

            var settings = new CatchUpSubscriptionSettings(
                maxLiveQueueSize: 10000,
                readBatchSize: 500,
                verboseLogging: false,
                resolveLinkTos: false,
                subscriptionName: "Tasks");

            subscription = _eventStore.SubscribeToAllFrom(
                lastCheckpoint: lastCheckpoint,
                settings: settings,
                eventAppeared: async (sub, @event) =>
                {
                    if (@event.OriginalEvent.EventType.StartsWith("$"))
                        return;

                    try
                    {
                        var eventType = Type.GetType(Encoding.UTF8.GetString(@event.OriginalEvent.Metadata));
                        var eventData = JsonSerializer.Deserialize(Encoding.UTF8.GetString(@event.OriginalEvent.Data), eventType);

                        if (eventType != typeof(CreatedTask) && eventType != typeof(AssignedTask) && eventType != typeof(MovedTask) && eventType != typeof(CompletedTask))
                            return;

                        _taskRepository.Save(eventData);

                        await _checkpointRepository.SaveAsync("tasks", @event.OriginalPosition.GetValueOrDefault());
                    }
                    catch (Exception exception)
                    {
                        _logger.LogError(exception, exception.Message);
                    }
                },
                liveProcessingStarted: (sub) =>
                {
                    _logger.LogInformation("{SubscriptionName} subscription started.", sub.SubscriptionName);
                },
                subscriptionDropped: (sub, subDropReason, exception) =>
                {
                    _logger.LogWarning("{SubscriptionName} dropped. Reason: {SubDropReason}.", sub.SubscriptionName, subDropReason);
                });
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            subscription.Stop();

            return Task.CompletedTask;
        }
    }
}
