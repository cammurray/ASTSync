using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Azure.Identity;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Graph.Beta;
using Microsoft.Graph.Beta.Models;

namespace ASTSync;

public static class Sync
{

    private static string _appClientId = Environment.GetEnvironmentVariable("AppClientId", EnvironmentVariableTarget.Process);
    private static string _appTenantId = Environment.GetEnvironmentVariable("AppTenantId", EnvironmentVariableTarget.Process);
    private static string _appSecret =Environment.GetEnvironmentVariable("AppSecret", EnvironmentVariableTarget.Process);
    
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
    
    [FunctionName("Sync")]
    public static async Task RunAsync([TimerTrigger("0 */15 * * * *")] TimerInfo myTimer, ILogger log)
    {
        
        // Validate required variables
        if (string.IsNullOrEmpty(_appClientId) || string.IsNullOrEmpty(_appTenantId) || string.IsNullOrEmpty(_appSecret))
            throw new Exception("AppClientID, AppTenantID, and AppSecret must be set");

        var GraphClient = GetGraphServicesClient();
        
        log.LogInformation($"C# Timer trigger function executed at: {DateTime.UtcNow}");
        Console.WriteLine(GetStorageConnection());

        await CreateRequiredTables();
        
        // Spin up the batch queue processor
        CancellationTokenSource batchQueueProcessorTokenSource = new CancellationTokenSource();
        Task taskBatchQueueProcessor = Task.Run(() => BatchQueueProcessor(batchQueueProcessorTokenSource.Token));
        
        // Sync Tenant Simulations
        var simulationIds = await GetTenantSimulations(GraphClient);

        // Sync Tenant Simulation Users
        foreach (string id in simulationIds)
        {
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

            // Wait for all submission tasks to complete
            await Task.WhenAll(SubmissionTasks);

            // Sleep for a second
            await Task.Delay(1000);
        }
        
        // Flush all queues if not empty
        while (!tableActionQueue_SimulationUsers.IsEmpty && !tableActionQueue_SimulationUserEvents.IsEmpty &&
               !tableActionQueue_SimulationUserEvents.IsEmpty)
        {
            await BatchQueueProcess(tableActionQueue_Simulations,
                new TableClient(GetStorageConnection(), "Simulations"), cancellationToken);
            await BatchQueueProcess(tableActionQueue_SimulationUsers,
                new TableClient(GetStorageConnection(), "SimulationUsers"), cancellationToken);
            await BatchQueueProcess(tableActionQueue_SimulationUserEvents,
                new TableClient(GetStorageConnection(), "SimulationUserEvents"), cancellationToken);
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
            Console.WriteLine($"Uploading batch {BatchTransactions.Count} to {tableClient.Name}");
            try
            {
                await tableClient.SubmitTransactionAsync(BatchTransactions);
            }
            catch (TableTransactionFailedException e)
            {
                TableTransactionAction failingAction = BatchTransactions.ToList()[e.FailedTransactionActionIndex.Value];
                Console.WriteLine(e.FailedTransactionActionIndex);
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
                var SimulationExistingTableItem = await tableClient.GetEntityAsync<TableEntity>("Simulations", sim.Id);
                
                // Get last sync time
                DateTime? LastUserSync = null;
                if (SimulationExistingTableItem.HasValue && SimulationExistingTableItem.Value.ContainsKey("LastUserSync"))
                    LastUserSync = DateTime.Parse(SimulationExistingTableItem.Value["LastUserSync"].ToString());
                
                // Add id to hashset. Get the existing Simulation Id
                if(LastUserSync is null || LastUserSync < DateTime.UtcNow.AddHours(-1))
                    SimulationIds.Add(sim.Id);
                
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
        
        var requestInformation =
            GraphClient.Security.AttackSimulation.Simulations[SimulationId].ToGetRequestInformation();

        requestInformation.URI = new Uri(requestInformation.URI.ToString() + "/report/simulationUsers");

        var results = await GraphClient.RequestAdapter.SendAsync<UserSimulationDetailsCollectionResponse>(requestInformation, UserSimulationDetailsCollectionResponse.CreateFromDiscriminatorValue);

        var pageIterator = Microsoft.Graph.PageIterator<UserSimulationDetails,UserSimulationDetailsCollectionResponse>
            .CreatePageIterator(GraphClient, results, async (userSimDetail) =>
            {
                // Add the table item
                tableActionQueue_SimulationUsers.Enqueue(new TableTransactionAction(TableTransactionActionType.UpdateReplace, new TableEntity(SimulationId, userSimDetail.SimulationUser?.UserId)
                {
                    {"SimulationUser_UserId", userSimDetail.SimulationUser?.UserId},
                    {"SimulationUser_Email", userSimDetail.SimulationUser?.Email},
                    {"CompromisedDateTime", userSimDetail.CompromisedDateTime},
                    {"ReportedPhishDateTime", userSimDetail.ReportedPhishDateTime},
                }));
                
                // Add simulation user events in to table
                if (userSimDetail.SimulationEvents is not null)
                {
                    foreach (var simulationUserEvents in userSimDetail.SimulationEvents)
                    {
                        tableActionQueue_SimulationUserEvents.Enqueue(new TableTransactionAction(TableTransactionActionType.UpdateReplace, new TableEntity(SimulationId, $"{userSimDetail.SimulationUser?.UserId}_{simulationUserEvents.EventName}_{simulationUserEvents.EventDateTime.Value.ToUnixTimeSeconds()}")
                        {
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
        tableActionQueue_Simulations.Enqueue(new TableTransactionAction(TableTransactionActionType.UpdateMerge, new TableEntity("Simulations", SimulationId)
        {
            {"LastUserSync", DateTime.UtcNow},
        }));
        
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
}