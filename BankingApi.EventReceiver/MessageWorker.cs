using BankingApi.EventReceiver.Errors;
using BankingApi.EventReceiver.Services;
using Microsoft.Extensions.Logging;

namespace BankingApi.EventReceiver
{
    public class MessageWorker
    {
        private readonly IServiceBusReceiver _serviceBusReceiver;
        private readonly IBankAccountService _bankAccountService;
        private readonly ILogger<MessageWorker> _logger;
        public MessageWorker(IServiceBusReceiver serviceBusReceiver, IBankAccountService bankAccountService, ILogger<MessageWorker> logger)
        {
            _serviceBusReceiver = serviceBusReceiver;
            _bankAccountService = bankAccountService;
            _logger = logger;
        }

        public async Task Start(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Event listenr service started");
            while (!cancellationToken.IsCancellationRequested)
            {
                var message = await _serviceBusReceiver.Peek(cancellationToken);

                if (message == null) return;

                try
                {
                    switch (message.MessageType)
                    {
                        case "Credit":
                            await _bankAccountService.CreditToAccount(message.BankAccountId, message.Amount, cancellationToken);
                            await _serviceBusReceiver.Complete(cancellationToken);
                            break;
                        case "Debit":
                            await _bankAccountService.DebitFromAccount(message.BankAccountId, message.Amount, cancellationToken);
                            await _serviceBusReceiver.Complete(cancellationToken);
                            break;
                        default:
                            await _serviceBusReceiver.MoveToDeadLetter(cancellationToken);
                            break;
                    }
                }
                catch (TransientException tex)
                {
                    _logger.LogWarning("Transient error occured, Error: {ERROR}, retrying...", tex.Message);
                    await _serviceBusReceiver.Abandon(cancellationToken);
                }
                catch (NonTransientException ntex)
                {
                    _logger.LogError("Non transient error occurred, Error: {ERROR}, moving message to dead leatter and exiting", ntex.Message);
                    await _serviceBusReceiver.MoveToDeadLetter(cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError("An unexpected error occurred, Error: {ERROR}, moving message to dead leatter and exiting", ex.Message);
                    await _serviceBusReceiver.MoveToDeadLetter(cancellationToken);
                }
            }
        }
    }
}
