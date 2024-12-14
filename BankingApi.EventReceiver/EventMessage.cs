using Newtonsoft.Json;

namespace BankingApi.EventReceiver;

public class EventMessage
{
    public Guid Id { get; set; }  // Unique identifier for the message
    public string Type { get; set; }  // Type of the event (e.g., "Credit", "Debit")
    public decimal Amount { get; set; }  // Amount involved in the transaction
    public string AccountId { get; set; }  // ID of the bank account

    // Method to convert a JSON string into an EventMessage object
    public static EventMessage FromJson(string json)
    {
        try
        {
            return JsonConvert.DeserializeObject<EventMessage>(json);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error deserializing EventMessage: {ex.Message}");
            return null;
        }
    }

    // Method to convert EventMessage object to JSON string (for logging or other purposes)
    public string ToJson()
    {
        return JsonConvert.SerializeObject(this);
    }

    // Override ToString for easy logging
    public override string ToString()
    {
        return $"EventMessage {{ Id: {Id}, Type: {Type}, Amount: {Amount}, AccountId: {AccountId} }}";
    }
}