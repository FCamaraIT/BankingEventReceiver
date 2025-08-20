using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace BankingApi.EventReceiver
{
    public class MessageWorker
    {
        private readonly IServiceBusReceiver _serviceBusReceiver;
        private readonly BankingApiDbContext _dbContext;
        private readonly ILogger<MessageWorker> _logger;

        public MessageWorker(IServiceBusReceiver serviceBusReceiver, DbContextOptions<BankingApiDbContext> dbContextOptions, ILogger<MessageWorker> logger)
        {
            _serviceBusReceiver = serviceBusReceiver;
            _dbContext = new BankingApiDbContext(dbContextOptions);
            _logger = logger;
        }

        public async Task Start()
        {
            while (true)
            {
                var message = await _serviceBusReceiver.Peek();
                if (message == null)
                {
                    _logger.LogInformation("No messages in queue. Waiting 10 seconds.");
                    await Task.Delay(TimeSpan.FromSeconds(10));
                    continue;
                }

                try
                {
                    var eventData = JsonSerializer.Deserialize<EventData>(message.MessageBody!);
                    if (eventData == null || (eventData.MessageType != "Credit" && eventData.MessageType != "Debit"))
                    {
                        _logger.LogWarning($"Invalid message type: {eventData?.MessageType}. Moving to deadletter.");
                        await _serviceBusReceiver.MoveToDeadLetter(message);
                        continue;
                    }

                    var account = await _dbContext.BankAccounts.FirstOrDefaultAsync(a => a.Id == eventData.BankAccountId);
                    if (account == null)
                    {
                        _logger.LogWarning($"BankAccount not found: {eventData.BankAccountId}. Moving to deadletter.");
                        await _serviceBusReceiver.MoveToDeadLetter(message);
                        continue;
                    }

                    if (eventData.MessageType == "Credit")
                    {
                        account.Balance += eventData.Amount;
                        _logger.LogInformation($"Credited {eventData.Amount} to account {account.Id}. New balance: {account.Balance}");
                    }
                    else if (eventData.MessageType == "Debit")
                    {
                        account.Balance -= eventData.Amount;
                        _logger.LogInformation($"Debited {eventData.Amount} from account {account.Id}. New balance: {account.Balance}");
                    }

                    await _dbContext.SaveChangesAsync();
                    await _serviceBusReceiver.Complete(message);
                }
                catch (DbUpdateConcurrencyException ex)
                {
                    _logger.LogError(ex, "Concurrency error while updating account. Retrying.");
                    if (message.ProcessingCount < 3)
                    {
                        // Exponential backoff: 5, 25, 125 seconds
                        int[] delays = { 5, 25, 125 };
                        int delay = delays[message.ProcessingCount];
                        await _serviceBusReceiver.ReSchedule(message, DateTime.UtcNow.AddSeconds(delay));
                    }
                    else
                    {
                        await _serviceBusReceiver.MoveToDeadLetter(message);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Non-transient error. Moving message to deadletter.");
                    await _serviceBusReceiver.MoveToDeadLetter(message);
                }
            }
        }

        private class EventData
        {
            public Guid Id { get; set; }
            public string MessageType { get; set; } = string.Empty;
            public Guid BankAccountId { get; set; }
            public decimal Amount { get; set; }
        }
    }
}
