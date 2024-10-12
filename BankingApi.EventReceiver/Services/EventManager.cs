using Azure.Messaging.ServiceBus;
using BankingApi.EventReceiver.Model;
using System.Text.Json;

namespace BankingApi.EventReceiver.Services
{
    public class EventManager : IServiceBusReceiver
    {
        private readonly ServiceBusClient _serviceBusClient;
        private readonly ServiceBusReceiver _serviceBusReceiver;
        private readonly string _queueName;
        private ServiceBusReceivedMessage _currentMessage;
        public EventManager()
        {
            _queueName = "";
            _serviceBusClient = new ServiceBusClient("", new ServiceBusClientOptions() { RetryOptions = new ServiceBusRetryOptions() { Mode = ServiceBusRetryMode.Exponential, Delay = TimeSpan.FromSeconds(5), MaxRetries = 3 } });
            _serviceBusReceiver = _serviceBusClient.CreateReceiver(_queueName);
        }

        public async Task<EventMessage?> Peek(CancellationToken cancellationToken)
        {
            //The service bus reciver is configured to wait 10 secs for a message before returning a null
            var receivedMessage = await _serviceBusReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), cancellationToken);

            if (receivedMessage != null)
            {
                _currentMessage = receivedMessage;
                var jsonMessage = receivedMessage.Body.ToString();
                var message = JsonSerializer.Deserialize<EventMessage>(jsonMessage);
                return message;
            }

            return null;
        }

        public async Task Abandon(CancellationToken cancellationToken)
        {
            await _serviceBusReceiver.AbandonMessageAsync(_currentMessage, cancellationToken: cancellationToken);
            _currentMessage = null;
        }

        public async Task Complete(CancellationToken cancellationToken)
        {
            await _serviceBusReceiver.CompleteMessageAsync(_currentMessage, cancellationToken);
            _currentMessage = null;
        }

        public async Task MoveToDeadLetter(CancellationToken cancellationToken)
        {
            await _serviceBusReceiver.DeadLetterMessageAsync(_currentMessage, cancellationToken: cancellationToken);
            _currentMessage = null;
        }
    }
}
