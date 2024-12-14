using Azure.Messaging.ServiceBus;
using System;
using System.Text.Json;
using System.Threading.Tasks;

namespace BankingApi.EventReceiver
{
    public class ServiceBusReceiverWrapper : IServiceBusReceiver
    {
        private readonly ServiceBusClient _serviceBusClient;
        private readonly ServiceBusReceiver _receiver;

        public ServiceBusReceiverWrapper(string connectionString, string queueName)
        {
            _serviceBusClient = new ServiceBusClient(connectionString);
            _receiver = _serviceBusClient.CreateReceiver(queueName, new ServiceBusReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock
            });
        }

        public async Task<EventMessage?> Peek()
        {
            try
            {
                // Peek the message from the queue
                ServiceBusReceivedMessage receivedMessage = await _receiver.PeekMessageAsync();
                if (receivedMessage != null)
                {
                    // Deserialize the body of the message into EventMessage
                    return EventMessage.FromJson(receivedMessage.Body.ToString());
                }
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error peeking message: {ex.Message}");
                return null;
            }
        }

        public async Task Abandon(EventMessage message)
        {
            try
            {
                // Find the corresponding ServiceBusReceivedMessage
                ServiceBusReceivedMessage serviceBusMessage = await GetServiceBusMessageById(message.Id);
                if (serviceBusMessage != null)
                {
                    // Abandon the message in Service Bus
                    await _receiver.AbandonMessageAsync(serviceBusMessage);
                    Console.WriteLine($"Message {message.Id} abandoned.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error abandoning message {message.Id}: {ex.Message}");
            }
        }

        public async Task Complete(EventMessage message)
        {
            try
            {
                // Find the corresponding ServiceBusReceivedMessage
                ServiceBusReceivedMessage serviceBusMessage = await GetServiceBusMessageById(message.Id);
                if (serviceBusMessage != null)
                {
                    // Complete the message in Service Bus
                    await _receiver.CompleteMessageAsync(serviceBusMessage);
                    Console.WriteLine($"Message {message.Id} completed.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error completing message {message.Id}: {ex.Message}");
            }
        }

        public async Task ReSchedule(EventMessage message, DateTime nextAvailableTime)
        {
            try
            {
                var delay = nextAvailableTime - DateTime.UtcNow;
                if (delay.TotalSeconds > 0)
                {
                    await Task.Delay(delay);
                    // Find the corresponding ServiceBusReceivedMessage
                    ServiceBusReceivedMessage serviceBusMessage = await GetServiceBusMessageById(message.Id);
                    if (serviceBusMessage != null)
                    {
                        await _receiver.AbandonMessageAsync(serviceBusMessage);
                        Console.WriteLine($"Message {message.Id} rescheduled for {nextAvailableTime}.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error rescheduling message {message.Id}: {ex.Message}");
            }
        }

        public async Task MoveToDeadLetter(EventMessage message)
        {
            try
            {
                // Find the corresponding ServiceBusReceivedMessage
                ServiceBusReceivedMessage serviceBusMessage = await GetServiceBusMessageById(message.Id);
                if (serviceBusMessage != null)
                {
                    // Move the message to the dead-letter queue
                    await _receiver.DeadLetterMessageAsync(serviceBusMessage);
                    Console.WriteLine($"Message {message.Id} moved to dead-letter queue.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error moving message {message.Id} to dead-letter: {ex.Message}");
            }
        }

        // Helper method to get the ServiceBusReceivedMessage by its Id
        private async Task<ServiceBusReceivedMessage> GetServiceBusMessageById(Guid messageId)
        {
            try
            {
                // Receive the message from the queue (you might want to filter by the messageId if needed)
                ServiceBusReceivedMessage receivedMessage = await _receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
                if (receivedMessage != null && receivedMessage.MessageId == messageId.ToString())
                {
                    return receivedMessage;
                }
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error retrieving message by Id {messageId}: {ex.Message}");
                return null;
            }
        }

        public async Task StopProcessingAsync()
        {
            await _receiver.CloseAsync();
            await _serviceBusClient.DisposeAsync();
        }
    }
}
