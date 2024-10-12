using BankingApi.EventReceiver.Model;

namespace BankingApi.EventReceiver.Services
{
    public interface IServiceBusReceiver
    {
        Task<EventMessage?> Peek(CancellationToken cancellationToken);

        Task Abandon(CancellationToken cancellationToken);
        
        Task Complete(CancellationToken cancellationToken);
        Task MoveToDeadLetter(CancellationToken cancellationToken);
    }
}
