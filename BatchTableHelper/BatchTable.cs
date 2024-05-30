using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging;

namespace ASTSync.BatchTableHelper;

/// <summary>
/// BatchTable Helper
///
/// The purpose of this helper is to queue table transaction actions until the batch size has been hit,
/// and then upload all transactions. Handle all batch errors by combination of re-queues and other error
/// handling.
/// </summary>
public class BatchTable : IAsyncDisposable
{

    /// <summary>
    /// Max batch size, default 100
    /// </summary>
    private int _BatchSize { get; set; }

    /// <summary>
    /// Name of table
    /// </summary>
    private string _TableName { get; set; }
    
    /// <summary>
    /// Batch upload task
    /// </summary>
    private Task _batchUploadTask { get; set; }

    /// <summary>
    /// Queue for batch operations 
    /// </summary>
    /// <returns></returns>
    private ConcurrentQueue<TableTransactionAction> _batchActions = new ConcurrentQueue<TableTransactionAction>();

    /// <summary>
    /// Logger
    /// </summary>
    private ILogger _log { get; set; }
    
    /// <summary>
    /// Table client
    /// </summary>
    private TableClient _tableClient { get; set; }
    
    /// <summary>
    /// Batch Table
    /// </summary>
    /// <param name="ConnectionString">Table Connection String</param>
    /// <param name="TableName">Table Name for batches</param>
    /// <param name="batchSize">Size of batches</param>
    /// <param name="logger">Logger for logging</param>
    public BatchTable(string ConnectionString, string TableName, int batchSize, ILogger logger)
    {
        
        // Establish service client
        var serviceClient = new TableServiceClient(ConnectionString);
        
        // Create required table
        serviceClient.CreateTableIfNotExists(TableName);
        
        // Set vars
        _log = logger;
        _BatchSize = batchSize;
        _TableName = TableName;
        
        _tableClient = new TableClient(ConnectionString, TableName);
    }

    /// <summary>
    /// Trigger upload task if one is not running
    /// </summary>
    private void TriggerUploadTask()
    {

        if (_batchUploadTask is not null && _batchUploadTask.IsFaulted)
        {
            _log.LogError($"batchUploadTask for table name {_TableName} has faulted: {_batchUploadTask.Exception}");
        }
        
        if (_batchUploadTask is null || _batchUploadTask.IsCompleted)
            _batchUploadTask = Task.Run(() => BatchUploadAsync());
    }
    
    /// <summary>
    /// Flush all remaining items in batch, or until timeout
    /// </summary>
    /// <param name="timeout">Timeout when task should return</param>
    /// <returns></returns>
    public async Task<bool> FlushBatchAsync(TimeSpan? timeout)
    {

        // Default to 5 minute timeout
        if (timeout is null)
            timeout = TimeSpan.FromMinutes(5);
        
        // Stop watch for timing dispose time
        Stopwatch sw = Stopwatch.StartNew();
        
        // If batch actions is not empty, continue to attempt to upload for 1 minute.
        while (!_batchActions.IsEmpty && sw.Elapsed < timeout)
        {
            TriggerUploadTask();
            
            // Delay task a second to allow task to complete, prevents locking thread
            await Task.Delay(TimeSpan.FromSeconds(1));
        }
            
        
        // If batch is still not empty, then log an error
        if (!_batchActions.IsEmpty)
            _log.LogError(
                $"TableBatchHelper instructed to upload now for {_TableName} has exceeded timeout with batch items left in the queue. This is most likely due to; items that could not be uploaded (invalid) or uploads taking longer than a {timeout} to complete.");

        // Return if actions is empty
        return _batchActions.IsEmpty;
    }
    
    /// <summary>
    /// Perform batch upload
    /// </summary>
    /// <returns></returns>
    private async Task<bool> BatchUploadAsync()
    {
        List<TableTransactionAction> BatchTransactions = new List<TableTransactionAction>();
        
        // Used to re-queue transactions that cannot be put in this batch
        // Such as transactions with a row key that is already present in the batch (cannot perform within the same batch)
        
        List<TableTransactionAction> RequeueTransactions = new List<TableTransactionAction>();

        // Take items out of the queue until it's empty or the max batch size hit
        while (!_batchActions.IsEmpty && BatchTransactions.Count < _BatchSize)
        {
            TableTransactionAction dequeued;

            if (_batchActions.TryDequeue(out dequeued))
            {
                // Validate row key is not already in batch transactions
                // Batches cannot contain two transactions for the same partition key and row.
                
                if (BatchTransactions.Any(x =>
                        x.Entity.PartitionKey == dequeued.Entity.PartitionKey &&
                        x.Entity.RowKey == dequeued.Entity.RowKey))
                {
                    // Requeue the transaction for next batch as it is already existing in this batch
                    RequeueTransactions.Add(dequeued);
                }
                else
                {
                    BatchTransactions.Add(dequeued);
                }
                
            }
                
        }

        if (BatchTransactions.Any())
        {
            try
            {
                await _tableClient.SubmitTransactionAsync(BatchTransactions);
            }
            catch (TableTransactionFailedException e)
            {
                List<TableTransactionAction> failedBatch = BatchTransactions.ToList();
                
                _log.LogError($"Failed to insert batch transaction in {_tableClient.Name} with partition key {failedBatch[e.FailedTransactionActionIndex.Value].Entity.PartitionKey} row key {failedBatch[e.FailedTransactionActionIndex.Value].Entity.RowKey} {e.Message}");
                
                // Remove the failing item from the batch and requeue rest
                failedBatch.RemoveAt(e.FailedTransactionActionIndex.Value);
                foreach (TableTransactionAction action in failedBatch)
                {
                    _batchActions.Enqueue(action);
                }
            }
        }
        
        // Requeue transactions that need to be moved to another batch
        if (RequeueTransactions.Any())
        {
            foreach(var transaction in RequeueTransactions)
                _batchActions.Enqueue(transaction);
        }

        return true;
    }

    /// <summary>
    /// Enqueue and upload when hits max size
    /// </summary>
    /// <returns></returns>
    public void EnqueueUpload(TableTransactionAction action)
    {
        // Enqueue
        _batchActions.Enqueue(action);
        
        // Run upload if > batch size
        if (_batchActions.Count >= _BatchSize)
            TriggerUploadTask();
    }
    
    /// <summary>
    /// Batch upload and dispose.
    ///
    /// This will attempt to flush the queue in 1 minute.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Stop watch for timing dispose time
        Stopwatch sw = Stopwatch.StartNew();
        
        // If batch actions is not empty, continue to attempt to upload for 1 minute.
        while (!_batchActions.IsEmpty && sw.Elapsed < TimeSpan.FromMinutes(1))
        {
            TriggerUploadTask();
            
            // Delay task a second to allow task to complete, prevents locking thread
            await Task.Delay(TimeSpan.FromSeconds(1));
        }
        
        // If batch is still not empty, then log an error
        if (!_batchActions.IsEmpty)
            _log.LogError(
                $"TableBatchHelper for {_TableName} has been disposed of with batch items left in the queue. This is most likely due to; items that could not be uploaded (invalid) or uploads taking longer than a minute to complete.");
        
    }
}