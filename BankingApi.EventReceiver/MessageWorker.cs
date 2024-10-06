using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace BankingApi.EventReceiver
{
    public class MessageWorker
    {
        private IServiceBusReceiver serviceBusReceiver;
        private BankingApiDbContext dbContext;
        private ILogger logger;
        public MessageWorker(IServiceBusReceiver serviceBusReceiver, BankingApiDbContext dbContext, ILogger logger)
        {
            this.serviceBusReceiver = serviceBusReceiver;
            this.dbContext = dbContext;
            this.logger = logger;
        }

        public async Task Start()
        {
            while (true)
            {
                try
                {
                    var message= await serviceBusReceiver.Peek();

                    //serviceBusReceiver.Peek returns null
                    if (message == null)
                    {
                        //wait 10 sec = 1000 ms
                        await Task.Delay(1000);
                    }
                    else
                    {
                        await HandleMessage(message);
                    }

                }
                catch (Exception ex) {
                    logger.LogInformation(ex,"Error while reading messages");
                }

            }
        }

        private async Task HandleMessage(EventMessage message)
        {
            try
            {
                var bankoperation = JsonSerializer.Deserialize<BankOperation>(message.Id);

                if (bankoperation == null)
                {
                    logger.LogInformation("Message is invalid");
                    await serviceBusReceiver.MoveToDeadLetter(message);
                    return;
                }

                if (bankoperation.MessageType == "Credit" || bankoperation.MessageType == "Debit")
                {
                    await UpdateBankAccount(bankoperation);
                    await serviceBusReceiver.Complete(message);

                }
                else
                {
                    await serviceBusReceiver.MoveToDeadLetter(message);
                }

            }
            //here it can be different exception, it could be other types that represent transient failure
            catch (DbUpdateException ex)
            {
                logger.LogWarning(ex, "Database update failed");
                
                if (message.ProcessingCount == 3)
                {
                    await serviceBusReceiver.Abandon(message);
                }

                //retried after 5 seconds
                else if (message.ProcessingCount == 0) {
                    await serviceBusReceiver.ReSchedule(message, DateTime.UtcNow.AddSeconds(5));
                }
                //retried after 25 seconds
                else if (message.ProcessingCount == 1)
                {
                    await serviceBusReceiver.ReSchedule(message, DateTime.UtcNow.AddSeconds(25));
                }
                //retried after 125 seconds.
                else if (message.ProcessingCount == 2)
                {
                    await serviceBusReceiver.ReSchedule(message, DateTime.UtcNow.AddSeconds(125));
                }

                message.ProcessingCount +=1;
            }
            catch (Exception ex)
            {
                logger.LogInformation(ex, "Error while handling the message");
            }
        }

        private async Task UpdateBankAccount(BankOperation bankoperation)
        {
            try
            {
                var bankaccount= await dbContext.BankAccounts.FirstOrDefaultAsync(b=>b.Id == bankoperation.BankAccountId);

                if (bankaccount == null)
                {
                    logger.LogInformation("Bank account having Id " + bankoperation.BankAccountId + " doesnt exist");
                    return;
                }

                if (bankoperation.MessageType== "Credit")
                {
                    bankaccount.Balance += bankoperation.Amount;
                }
                else if (bankoperation.MessageType== "Debit")
                {
                    if (bankaccount.Balance >= bankoperation.Amount)
                    {
                        bankaccount.Balance -= bankoperation.Amount;
                    }
                    else
                    {
                        throw new Exception("Insufficient balance");
                    }
                }

                dbContext.BankAccounts.Update(bankaccount);
                await dbContext.SaveChangesAsync();

            }
            catch (Exception ex)
            {
                logger.LogInformation(ex, "Error updating the bank account");
            }
        }
    }

    public class TransientFailureException : Exception
    {
        public TransientFailureException(string message, Exception innerException) : base(message, innerException) { }
    }
}
