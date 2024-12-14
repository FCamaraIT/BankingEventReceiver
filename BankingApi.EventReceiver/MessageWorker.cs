using Microsoft.EntityFrameworkCore;

namespace BankingApi.EventReceiver
{
    public class MessageWorker
    {
        private readonly IServiceBusReceiver _serviceBusReceiver;
        private readonly BankingApiDbContext _dbContext;
        private const int MaxRetries = 3;

        public MessageWorker(IServiceBusReceiver serviceBusReceiver, BankingApiDbContext dbContext)
        {
            _serviceBusReceiver = serviceBusReceiver;
            _dbContext = dbContext;
        }

        public async Task Start()
        {
            while (true)
            {
                try
                {
                    var message = await _serviceBusReceiver.Peek();

                    if (message == null)
                    {
                        // No messages, await for 10 seconds
                        await Task.Delay(10000);
                        continue;
                    }

                    await ProcessMessageWithRetries(message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unexpected error: {ex.Message}");
                }
            }
        }

        private async Task ProcessMessageWithRetries(EventMessage message)
        {
            int retryCount = 0;

            while (retryCount <= MaxRetries)
            {
                try
                {
                    // Attempt to process the message (e.g., update bank account balance)
                    await ProcessMessage(message);

                    // If processing is successful, complete the message
                    await _serviceBusReceiver.Complete(message);
                    return;  // Exit the loop if the message is processed successfully
                }
                catch (TransientException ex)
                {
                    // For transient exceptions, retry with exponential backoff
                    retryCount++;
                    int delayInSeconds = GetExponentialBackoffDelay(retryCount);

                    Console.WriteLine($"Transient error occurred: {ex.Message}. Retrying in {delayInSeconds} seconds... (Attempt {retryCount}/{MaxRetries})");

                    if (retryCount > MaxRetries)
                    {
                        // If max retries are exceeded, move to dead-letter queue
                        await _serviceBusReceiver.MoveToDeadLetter(message);
                        Console.WriteLine($"Message {message.Id} exceeded max retries and moved to dead-letter.");
                        return;
                    }

                    // Wait for the specified exponential backoff delay
                    await Task.Delay(TimeSpan.FromSeconds(delayInSeconds));
                    await ProcessMessage(message);
                }
                catch (NonTransientException ex)
                {
                    // For non-transient errors, immediately move to dead-letter queue
                    Console.WriteLine($"Non-transient error: {ex.Message}. Moving message to dead-letter.");
                    await _serviceBusReceiver.MoveToDeadLetter(message);
                    return;
                }
            }
        }

        private async Task ProcessMessage(EventMessage message)
        {
            // Start a database transaction to ensure consistency
            using (var transaction = await _dbContext.Database.BeginTransactionAsync())
            {
                try
                {
                    var account = await _dbContext.BankAccounts.FirstOrDefaultAsync(a => a.AccountId.ToString() == message.AccountId);

                    if (account == null)
                    {
                        // Handle account not found (possibly move to dead-letter or log error)
                        throw new NonTransientException($"Account {message.AccountId} not found.");
                    }

                    // Process the transaction
                    if (message.Type == "Credit")
                    {
                        account.Balance += message.Amount;  // Credit operation
                    }
                    else if (message.Type == "Debit")
                    {
                        account.Balance -= message.Amount;  // Debit operation
                    }
                    else
                    {
                        throw new NonTransientException("Invalid message type.");
                    }

                    // Save changes and commit transaction
                    await _dbContext.SaveChangesAsync();
                    await transaction.CommitAsync();  // Commit the transaction
                    Console.WriteLine($"Transaction processed: {message.Type} of {message.Amount} for account {message.AccountId}");
                }
                catch (Exception ex)
                {
                    // If any error occurs, rollback the transaction
                    await transaction.RollbackAsync();
                    Console.WriteLine($"Error processing message: {ex.Message}");
                    throw new TransientException("Transient error occurred during transaction processing.", ex);
                }
            }
        }

        private int GetExponentialBackoffDelay(int retryCount)
        {
            // Exponential backoff with delays: 5s, 25s, 125s
            switch (retryCount)
            {
                case 1:
                    return 5;
                case 2:
                    return 25;
                case 3:
                    return 125;
                default:
                    return 0;  // No delay after the last retry
            }
        }
    }


}
