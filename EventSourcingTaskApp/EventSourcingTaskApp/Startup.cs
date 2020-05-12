namespace EventSourcingTaskApp
{
    using Couchbase.Extensions.DependencyInjection;
    using EventSourcingTaskApp.HostedServices;
    using EventSourcingTaskApp.Infrastructure;
    using EventStore.ClientAPI;
    using Microsoft.AspNetCore.Builder;
    using Microsoft.AspNetCore.Hosting;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.Hosting;

    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            var eventStoreConnection = EventStoreConnection.Create(
                connectionString: Configuration.GetValue<string>("EventStore:ConnectionString"),
                builder: ConnectionSettings.Create().KeepReconnecting(),
                connectionName: Configuration.GetValue<string>("EventStore:ConnectionName"));

            eventStoreConnection.ConnectAsync().GetAwaiter().GetResult();

            services.AddSingleton(eventStoreConnection);
            services.AddTransient<AggregateRepository>();

            services.AddCouchbase((opt) =>
            {
                opt.ConnectionString = Configuration.GetValue<string>("Couchbase:ConnectionString");
                opt.Username = Configuration.GetValue<string>("Couchbase:Username");
                opt.Password = Configuration.GetValue<string>("Couchbase:Password");
            });

            services.AddTransient<CheckpointRepository>();
            services.AddTransient<TaskRepository>();

            services.AddHostedService<TaskHostedService>();

            services.AddControllers();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
