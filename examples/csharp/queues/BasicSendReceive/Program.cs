// Example: queues/BasicSendReceive — send 3 queue messages, receive one at a time.
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var channel = "csharp-ce-queues.basic";
var clientId = "kubemq-ce-csharp-worker";
var formatter = new JsonEventFormatter();
using var httpClient = new HttpClient();

Console.WriteLine($"Sending 3 messages to queue '{channel}':");
for (int i = 1; i <= 3; i++)
{
    var ev = new CloudEvent {
        Id = Guid.NewGuid().ToString(),
        Type = "com.kubemq.examples.queues.task",
        Source = new Uri("urn:kubemq-ce-csharp-example"),
        Subject = channel,
        DataContentType = "application/json",
        Data = new { task_id = i, task = "process-item" },
    };
    var bytes = formatter.EncodeStructuredModeMessage(ev, out var ct);
    using var c = new ByteArrayContent(bytes.ToArray());
    c.Headers.ContentType = MediaTypeHeaderValue.Parse(ct.ToString());
    var r = await httpClient.PostAsync($"{base_}/ce/queue/send", c);
    var j = JsonSerializer.Deserialize<JsonElement>(await r.Content.ReadAsStringAsync());
    Console.WriteLine($"  Sent task {i}: is_error={j.GetProperty("is_error")}");
}

Console.WriteLine("\nReceiving 3 messages:");
for (int i = 0; i < 3; i++)
{
    var url = $"{base_}/ce/queue/receive?channel={Uri.EscapeDataString(channel)}&client_id={clientId}&max_messages=1&wait_timeout=5";
    var r = await httpClient.PostAsync(url, null);
    var j = JsonSerializer.Deserialize<JsonElement>(await r.Content.ReadAsStringAsync());
    if (j.GetProperty("is_error").GetBoolean()) {
        Console.WriteLine($"  Error: {j.GetProperty("message")}"); continue;
    }
    if (j.TryGetProperty("data", out var data) && data.TryGetProperty("messages", out var msgs))
    {
        foreach (var m in msgs.EnumerateArray())
            Console.WriteLine($"  Received: type={m.GetProperty("type")} data={m.GetProperty("data")}");
    }
}

// Expected output:
// Sending 3 messages to queue 'csharp-ce-queues.basic':
//   Sent task 1: is_error=False
//   Sent task 2: is_error=False
//   Sent task 3: is_error=False
//
// Receiving 3 messages:
//   Received: type=com.kubemq.examples.queues.task data={"task_id":1,"task":"process-item"}
//   Received: type=com.kubemq.examples.queues.task data={"task_id":2,"task":"process-item"}
//   Received: type=com.kubemq.examples.queues.task data={"task_id":3,"task":"process-item"}
