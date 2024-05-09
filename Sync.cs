using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Azure.Identity;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Beta;
using Microsoft.Graph.Beta.Models;
using Microsoft.Graph.Beta.Models.ODataErrors;

namespace ASTSync;

public static class Sync
{

    private static string _appClientId = Environment.GetEnvironmentVariable("AppClientId", EnvironmentVariableTarget.Process);
    private static string _appTenantId = Environment.GetEnvironmentVariable("AppTenantId", EnvironmentVariableTarget.Process);
    private static string _appSecret = Environment.GetEnvironmentVariable("AppSecret", EnvironmentVariableTarget.Process);

    // If to pull entra users
    private static bool _pullEntraUsers =
        bool.Parse(Environment.GetEnvironmentVariable("SyncEntra", EnvironmentVariableTarget.Process) ?? "false");
    
    /// <summary>
    /// How many table rows to send up in a batch
    /// </summary>
    private static int _maxTableBatchSize = 100;

    /// <summary>
    /// Queue for Simulation Table entries, async batch/bulk processed
    /// </summary>
    private static ConcurrentQueue<TableTransactionAction> tableActionQueue_Simulations = new();
    
    /// <summary>
    /// Queue for Simulation User Table entries, async batch/bulk processed
    /// </summary>
    private static ConcurrentQueue<TableTransactionAction> tableActionQueue_SimulationUsers = new();
    
    /// <summary>
    /// Queue for Simulation User Events Table entries, async batch/bulk processed
    /// </summary>
    private static ConcurrentQueue<TableTransactionAction> tableActionQueue_SimulationUserEvents  = new();
    
    /// <summary>
    /// Queue for User entries, async batch/bulk processed
    /// </summary>
    private static ConcurrentQueue<TableTransactionAction> tableActionQueue_Users  = new();

    /// <summary>
    /// Maintains a list of users we have already synced
    /// </summary>
    private static ConcurrentDictionary<string, bool> userListSynced = new();
    
    /// <summary>
    /// Logger
    /// </summary>
    private static ILogger _log { get; set; }
    
    [FunctionName("Sync")]
    public static async Task RunAsync([TimerTrigger("0 */15 * * * *")] TimerInfo myTimer, ILogger log)
    {

        _log = log;
        
        // Validate required variables
        if (string.IsNullOrEmpty(_appClientId) || string.IsNullOrEmpty(_appTenantId) || string.IsNullOrEmpty(_appSecret))
            throw new Exception("AppClientID, AppTenantID, and AppSecret must be set");

        var GraphClient = GetGraphServicesClient();
        
        _log.LogInformation($"C# Timer trigger function executed at: {DateTime.UtcNow}");

        await CreateRequiredTables();
        
        // Spin up the batch queue processor
        CancellationTokenSource batchQueueProcessorTokenSource = new CancellationTokenSource();
        Task taskBatchQueueProcessor = Task.Run(() => BatchQueueProcessor(batchQueueProcessorTokenSource.Token));
        
        // Sync Tenant Simulations to perform sync whilst sync'ing simulations to table
        // This can probably be moved to an async foreach below.
        
        var simulationIds = await GetTenantSimulations(GraphClient);

        // Sync Tenant Simulation Users
        foreach (string id in simulationIds)
        {
            // Throw if taskBatchQueueProcessor is faulted
            if (taskBatchQueueProcessor.IsFaulted)
                throw new Exception($"taskBatchQueueProcessor has faulted: {taskBatchQueueProcessor.Exception}");
            
            // Run get simulation users
            await GetTenantSimulationUsers(GraphClient, id);
        }

        // Wait for batch queue processor to complete
        while (!taskBatchQueueProcessor.IsCompleted)
        {
            // Signal to batch queue processor work has completed
            batchQueueProcessorTokenSource.Cancel();

            // Sleep for a second
            await Task.Delay(1000);
        }
    }

    /// <summary>
    /// Processes the queues 
    /// </summary>
    private static async Task BatchQueueProcessor(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // Tasks for submission tasks
            List<Task> SubmissionTasks = new List<Task>();
        
            // Process Simulations Queue
            if (tableActionQueue_Simulations.Count > _maxTableBatchSize)
                SubmissionTasks.Add(Task.Run(() => BatchQueueProcess(tableActionQueue_Simulations,
                    new TableClient(GetStorageConnection(), "Simulations"), cancellationToken))); 
        
            // Process SimulationUsers Queue
            if (tableActionQueue_SimulationUsers.Count > _maxTableBatchSize)
                SubmissionTasks.Add(Task.Run(() => BatchQueueProcess(tableActionQueue_SimulationUsers,
                    new TableClient(GetStorageConnection(), "SimulationUsers"), cancellationToken))); 
            
            // Process SimulationUserEvents Queue
            if (tableActionQueue_SimulationUserEvents.Count > _maxTableBatchSize)
                SubmissionTasks.Add(Task.Run(() => BatchQueueProcess(tableActionQueue_SimulationUserEvents,
                    new TableClient(GetStorageConnection(), "SimulationUserEvents"), cancellationToken))); 
            
            // Process Users Queue
            if (tableActionQueue_Users.Count > _maxTableBatchSize)
                SubmissionTasks.Add(Task.Run(() => BatchQueueProcess(tableActionQueue_Users,
                    new TableClient(GetStorageConnection(), "Users"), cancellationToken))); 

            // Wait for all submission tasks to complete
            await Task.WhenAll(SubmissionTasks);

            // Sleep for a second
            await Task.Delay(1000);
        }
        
        // Flush all queues if not empty
        while (!tableActionQueue_SimulationUsers.IsEmpty && !tableActionQueue_SimulationUserEvents.IsEmpty &&
               !tableActionQueue_SimulationUserEvents.IsEmpty && !tableActionQueue_Users.IsEmpty)
        {
            await BatchQueueProcess(tableActionQueue_Simulations,
                new TableClient(GetStorageConnection(), "Simulations"), cancellationToken);
            await BatchQueueProcess(tableActionQueue_SimulationUsers,
                new TableClient(GetStorageConnection(), "SimulationUsers"), cancellationToken);
            await BatchQueueProcess(tableActionQueue_SimulationUserEvents,
                new TableClient(GetStorageConnection(), "SimulationUserEvents"), cancellationToken);
            await BatchQueueProcess(tableActionQueue_Users,
                new TableClient(GetStorageConnection(), "Users"), cancellationToken);
        }

    }

    /// <summary>
    /// Processes a queue
    /// </summary>
    private static async Task BatchQueueProcess(ConcurrentQueue<TableTransactionAction> Queue, TableClient tableClient, CancellationToken ct)
    {
        List<TableTransactionAction> BatchTransactions = new List<TableTransactionAction>();

        // Take items out of the queue until it's empty or the max batch size hit
        while (!Queue.IsEmpty && BatchTransactions.Count < _maxTableBatchSize)
        {
            TableTransactionAction dequeued;

            if (Queue.TryDequeue(out dequeued))
                BatchTransactions.Add(dequeued);
        }

        if (BatchTransactions.Any())
        {
            // Submit the transactions
            _log.LogInformation($"Uploading batch to {tableClient.Name} of size {BatchTransactions.Count}");
            
            try
            {
                await tableClient.SubmitTransactionAsync(BatchTransactions);
            }
            catch (TableTransactionFailedException e)
            {
                TableTransactionAction failingAction = BatchTransactions.ToList()[e.FailedTransactionActionIndex.Value];
                _log.LogError($"Failed to insert batch transaction in {tableClient.Name} with partition key {failingAction.Entity.PartitionKey} row key {failingAction.Entity.RowKey}");
            }
        }
    }
    
    /// <summary>
    /// Create required tables
    /// </summary>
    private static async Task CreateRequiredTables()
    {
        // Establish service client
        var serviceClient = new TableServiceClient(GetStorageConnection());
        
        // Create required tables
        await serviceClient.CreateTableIfNotExistsAsync("Simulations");
        await serviceClient.CreateTableIfNotExistsAsync("SimulationUsers");
        await serviceClient.CreateTableIfNotExistsAsync("SimulationUserEvents");
        await serviceClient.CreateTableIfNotExistsAsync("Users");
    }
    
    /// <summary>
    /// Get Simulations for Tenant
    /// </summary>
    /// <param name="GraphClient"></param>
    private static async Task<HashSet<string>> GetTenantSimulations(GraphServiceClient GraphClient)
    {
        
        // Simulation Ids
        HashSet<string> SimulationIds = new HashSet<string>();
        
        // Get table client for table
        TableClient tableClient = new TableClient(GetStorageConnection(), "Simulations");

        // Get simulation results
        var results = await GraphClient
            .Security
            .AttackSimulation
            .Simulations
            .GetAsync((requestConfiguration) =>
            {
                requestConfiguration.QueryParameters.Top = 1000;
            });

        var pageIterator = Microsoft.Graph.PageIterator<Simulation,SimulationCollectionResponse>
            .CreatePageIterator(GraphClient, results, async (sim) =>
            {
                // Get the table row item for this simulation
                var SimulationExistingTableItem = await tableClient.GetEntityIfExistsAsync<TableEntity>("Simulations", sim.Id);

                // For determining the last user sync
                DateTime? LastUserSync = null;
                if (SimulationExistingTableItem.HasValue && SimulationExistingTableItem.Value.ContainsKey("LastUserSync"))
                    LastUserSync = DateTime.SpecifyKind(DateTime.Parse(SimulationExistingTableItem.Value["LastUserSync"].ToString()), DateTimeKind.Utc);
                
                // Perform a user sync (if)
                // - We have never performed a sync
                // - The simulation finished within the past 7 days
                // - Or the simulation is running
                // - Last user sync is more than a month ago
                
                if (!SimulationExistingTableItem.HasValue || 
                    sim.Status == SimulationStatus.Running || 
                    sim.CompletionDateTime > DateTime.UtcNow.AddDays(-7) || 
                    LastUserSync is null || 
                    (LastUserSync.HasValue && LastUserSync.Value < DateTime.UtcNow.AddMonths(-1)))
                {
                    _log.LogInformation($"Perform full synchronisation of simulation '{sim.DisplayName}' status {SimulationStatus.Running}");
                    SimulationIds.Add(sim.Id);
                }
                
                // Add the table item
                tableActionQueue_Simulations.Enqueue(new TableTransactionAction(TableTransactionActionType.UpdateReplace, new TableEntity("Simulations", sim.Id)
                {
                    {"AttackTechnique", sim.AttackTechnique.ToString()},
                    {"AttackType", sim.AttackType.ToString()},
                    {"AutomationId", sim.AutomationId},
                    {"CompletionDateTime", sim.CompletionDateTime},
                    {"CreatedBy_Id", sim.CreatedBy?.Id},
                    {"CreatedBy_DisplayName", sim.CreatedBy?.DisplayName},
                    {"CreatedBy_Email", sim.CreatedBy?.Email},
                    {"CreatedDateTime", sim.CreatedDateTime},
                    {"Description",sim.Description},
                    {"DisplayName",sim.DisplayName},
                    {"DurationInDays", sim.DurationInDays},
                    {"IsAutomated", sim.IsAutomated},
                    {"LastModifiedBy_Id", sim.LastModifiedBy?.Id},
                    {"LastModifiedBy_DisplayName", sim.LastModifiedBy?.DisplayName},
                    {"LastModifiedBy_Email", sim.LastModifiedBy?.Email},
                    {"LastModifiedDateTime", sim.LastModifiedDateTime},
                    {"Payload_Id", sim.Payload?.Id},
                    {"Payload_DisplayName", sim.Payload?.DisplayName},
                    {"Payload_Platform", sim.Payload?.Platform?.ToString()},
                    {"Status", sim.Status.ToString()},
                    {"AutomationId", sim.AutomationId},
                    {"LastUserSync", LastUserSync}
                }));
                
                return true; 
            });

        await pageIterator.IterateAsync();
        
        return SimulationIds;
    }

    /// <summary>
    /// Get Simulations Users
    /// </summary>
    /// <param name="GraphClient"></param>
    private static async Task GetTenantSimulationUsers(GraphServiceClient GraphClient, string SimulationId)
    {
        Stopwatch sw = Stopwatch.StartNew();
        _log.LogInformation($"Performing full user synchronisation of {SimulationId}");

        var requestInformation =
            GraphClient.Security.AttackSimulation.Simulations[SimulationId].ToGetRequestInformation();

        requestInformation.URI = new Uri(requestInformation.URI.ToString() + "/report/simulationUsers");
        requestInformation.QueryParameters["Top"] = 1000;

        var results = await GraphClient.RequestAdapter.SendAsync<UserSimulationDetailsCollectionResponse>(requestInformation, UserSimulationDetailsCollectionResponse.CreateFromDiscriminatorValue);

        var pageIterator = Microsoft.Graph.PageIterator<UserSimulationDetails,UserSimulationDetailsCollectionResponse>
            .CreatePageIterator(GraphClient, results, async (userSimDetail) =>
            {
                // Add the table item
                tableActionQueue_SimulationUsers.Enqueue(new TableTransactionAction(TableTransactionActionType.UpdateReplace, new TableEntity(SimulationId, userSimDetail.SimulationUser?.UserId)
                {
                    {"SimulationUser_Id", $"{SimulationId}-{userSimDetail.SimulationUser?.UserId}"},
                    {"SimulationUser_UserId", userSimDetail.SimulationUser?.UserId},
                    {"SimulationUser_Email", userSimDetail.SimulationUser?.Email},
                    {"CompromisedDateTime", userSimDetail.CompromisedDateTime},
                    {"ReportedPhishDateTime", userSimDetail.ReportedPhishDateTime},
                    {"AssignedTrainingsCount", userSimDetail.AssignedTrainingsCount},
                    {"CompletedTrainingsCount", userSimDetail.CompletedTrainingsCount},
                    {"InProgressTrainingsCount", userSimDetail.InProgressTrainingsCount},
                    {"IsCompromised", userSimDetail.IsCompromised},
                }));
                
                // Determine if should sync user
                if (await ShouldSyncUser(userSimDetail.SimulationUser?.UserId))
                {
                    await SyncUser(GraphClient, userSimDetail.SimulationUser?.UserId);
                }
                
                // Add simulation user events in to table
                if (userSimDetail.SimulationEvents is not null)
                {
                    foreach (var simulationUserEvents in userSimDetail.SimulationEvents)
                    {
                        tableActionQueue_SimulationUserEvents.Enqueue(new TableTransactionAction(TableTransactionActionType.UpdateReplace, new TableEntity(SimulationId, $"{userSimDetail.SimulationUser?.UserId}_{simulationUserEvents.EventName}_{simulationUserEvents.EventDateTime.Value.ToUnixTimeSeconds()}")
                        {
                            {"SimulationUser_Id", $"{SimulationId}-{userSimDetail.SimulationUser?.UserId}"},
                            {"SimulationUser_UserId", userSimDetail.SimulationUser?.UserId},
                            {"SimulationUserEvent_EventName", simulationUserEvents.EventName},
                            {"SimulationUserEvent_EventDateTime", simulationUserEvents.EventDateTime},
                            {"SimulationUserEvent_Browser", simulationUserEvents.Browser},
                            {"SimulationUserEvent_IpAddress", simulationUserEvents.IpAddress},
                            {"SimulationUserEvent_OsPlatformDeviceDetails", simulationUserEvents.OsPlatformDeviceDetails},
                        }));

                    }

                }
                
                return true; 
            });

        await pageIterator.IterateAsync();
        
        // update in the Simulations table that this has been syncd
        tableActionQueue_Simulations.Enqueue(new TableTransactionAction(TableTransactionActionType.UpsertMerge, new TableEntity("Simulations", SimulationId)
        {
            {"LastUserSync", DateTime.UtcNow},
        }));
        
        _log.LogInformation($"Full user synchronisation of {SimulationId} completed in {sw.Elapsed}");
        
        // Perform a task delay here to allow other threads to complete
        await Task.Delay(1000);

    }

    /// <summary>
    /// Get the Graph Client for Tenant
    /// </summary>
    /// <returns></returns>
    private static GraphServiceClient GetGraphServicesClient()
    {
        // Construct auth provider to Graph
        var scopes = new[] { "https://graph.microsoft.com/.default" };
        var tenantId = "common";
        
        var options = new TokenCredentialOptions
        {
            AuthorityHost = AzureAuthorityHosts.AzurePublicCloud
        };

        var clientSecretCredential = new ClientSecretCredential(_appTenantId, _appClientId, _appSecret, options);
        
        return new GraphServiceClient(clientSecretCredential, scopes);
    }
    
    /// <summary>
    /// Get Storage Connection from App settings
    /// </summary>
    /// <returns></returns>
    public static string GetStorageConnection() => Environment.GetEnvironmentVariable("AzureWebJobsStorage", EnvironmentVariableTarget.Process);

    /// <summary>
    /// Synchronise user
    /// </summary>
    /// <param name="id"></param>
    /// <returns></returns>
    private static async Task SyncUser(GraphServiceClient GraphClient, string id)
    {
        // Set in dictionary
        userListSynced[id] = true;
        
        try
        {
            var User = await GraphClient.Users[id].GetAsync();

            if (User is not null)
            {
                tableActionQueue_Users.Enqueue(new TableTransactionAction(TableTransactionActionType.UpdateReplace, new TableEntity("Users", id)
                {
                    {"DisplayName", User.DisplayName},
                    {"GivenName", User.GivenName},
                    {"Surname", User.Surname},
                    {"Country", User.Country},
                    {"Mail", User.Mail},
                    {"Department", User.Department},
                    {"CompanyName", User.CompanyName},
                    {"City", User.City},
                    {"Country", User.Country},
                    {"JobTitle", User.JobTitle},
                    {"LastUserSync", DateTime.UtcNow},
                    {"Exists", "true"},
                }));
            }

        }
        catch (ODataError e)
        {
            if (e.Error.Code == "Request_ResourceNotFound")
            {
                // User no longer exists
                tableActionQueue_Users.Enqueue(new TableTransactionAction(TableTransactionActionType.UpsertMerge, new TableEntity("Users", id)
                {
                    {"Exists", "false"},
                }));
            }
            else
            {
                _log.LogError($"Failed to sync user {id}: {e}");
            }
            
        }
    }
    
    /// <summary>
    /// Determine if should sync user
    /// </summary>
    /// <param name="id"></param>
    /// <returns></returns>
    private static async Task<bool> ShouldSyncUser(string id)
    {
        // Return false if set not to sync users
        if (!_pullEntraUsers)
            return false;
        
        // Return false if already synchronised
        if (userListSynced.ContainsKey(id))
            return false;
        
        // Get the table entity to determine how long ago the user has been pulled
        TableClient tableClient = new TableClient(GetStorageConnection(), "Users");
        var UserTableItem = await tableClient.GetEntityIfExistsAsync<TableEntity>("Users", id);
                
        // Get last sync time
        DateTime? LastUserSync = null;
        if (UserTableItem.HasValue && UserTableItem.Value.ContainsKey("LastUserSync"))
            LastUserSync = DateTime.SpecifyKind(DateTime.Parse(UserTableItem.Value["LastUserSync"].ToString()), DateTimeKind.Utc);

        // If no sync or days is older than a week
        if (LastUserSync is null || LastUserSync.Value < DateTime.UtcNow.AddDays(-7))
            return true;
           
        // Add to userSyncList so we don't need to check again
        userListSynced[id] = true;

        return false;

    }
}