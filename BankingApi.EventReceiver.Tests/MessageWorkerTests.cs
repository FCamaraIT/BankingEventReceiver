using System;
using System.Threading.Tasks;
using Xunit;
using Moq;
using Microsoft.Extensions.Logging;
using BankingApi.EventReceiver;
using Microsoft.EntityFrameworkCore;

public class MessageWorkerTests
{
    [Fact]
    public async Task Processes_Credit_Message_Updates_Balance_And_Completes_Message()
    {
        // Arrange
        var accountId = Guid.NewGuid();
        var bankAccount = new BankAccount { Id = accountId, Balance = 100 };
        var options = new DbContextOptionsBuilder<BankingApiDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString())
            .Options;
        var dbContext = new BankingApiDbContext(options);
        dbContext.BankAccounts.Add(bankAccount);
        dbContext.SaveChanges();
        var serviceBusMock = new Mock<IServiceBusReceiver>();
        var loggerMock = new Mock<ILogger<MessageWorker>>();
        var eventData = new {
            id = Guid.NewGuid(),
            messageType = "Credit",
            bankAccountId = accountId,
            amount = 50.0m
        };
        var message = new EventMessage {
            Id = Guid.NewGuid(),
            MessageBody = System.Text.Json.JsonSerializer.Serialize(eventData),
            ProcessingCount = 0
        };
        serviceBusMock.Setup(s => s.Peek()).ReturnsAsync(message);
        serviceBusMock.Setup(s => s.Complete(message)).Returns(Task.CompletedTask);
        var worker = new MessageWorker(
            serviceBusMock.Object,
            options,
            loggerMock.Object
        );
        typeof(MessageWorker).GetField("_dbContext", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).SetValue(worker, dbContext);

        // Act
        var task = worker.Start();
        await Task.Delay(100); // Let the worker process one message
        task.Dispose(); // Stop the infinite loop

        // Assert
        Assert.Equal(150, bankAccount.Balance);
        serviceBusMock.Verify(s => s.Complete(message), Times.Once);
    }
}
