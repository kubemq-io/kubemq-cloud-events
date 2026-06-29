// Example: events/ContentModes — structured and binary mode.
// Run: dotnet run
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using System.Net.Http.Headers;
using System.Text.Json;

static string ServerUrl() => Environment.GetEnvironmentVariable("KUBEMQ_CE_URL") ?? "http://localhost:9090";

var base_ = ServerUrl();
var formatter = new JsonEventFormatter();
using var httpClient = new HttpClient();

var cloudEvent = new CloudEvent
{
    Id = Guid.NewGuid().ToString(),
    Type = "com.kubemq.examples.events.content-mode",
    Source = new Uri("urn:kubemq-ce-csharp-example"),
    Subject = "csharp-ce-events.content-modes",
    DataContentType = "application/json",
    Data = new { message = "hello content modes" },
};

// Structured mode
Console.WriteLine("Sending in structured mode:");
var structuredBytes = formatter.EncodeStructuredModeMessage(cloudEvent, out var structuredCt);
using var sc = new ByteArrayContent(structuredBytes.ToArray());
sc.Headers.ContentType = MediaTypeHeaderValue.Parse(structuredCt.ToString());
var r1 = await httpClient.PostAsync($"{base_}/ce/send/event", sc);
var j1 = JsonSerializer.Deserialize<JsonElement>(await r1.Content.ReadAsStringAsync());
Console.WriteLine($"[structured] status={r1.StatusCode} is_error={j1.GetProperty("is_error")}");

// Binary mode
Console.WriteLine("\nSending in binary mode:");
using var binaryContent = new StringContent(
    JsonSerializer.Serialize(new { message = "hello binary mode" }),
    System.Text.Encoding.UTF8, "application/json");
using var binaryRequest = new HttpRequestMessage(HttpMethod.Post, $"{base_}/ce/send/event") { Content = binaryContent };
binaryRequest.Headers.Add("ce-specversion", "1.0");
binaryRequest.Headers.Add("ce-type", cloudEvent.Type);
binaryRequest.Headers.Add("ce-source", cloudEvent.Source!.ToString());
binaryRequest.Headers.Add("ce-id", cloudEvent.Id);
binaryRequest.Headers.Add("ce-subject", cloudEvent.Subject);
binaryRequest.Headers.Add("ce-time", DateTimeOffset.UtcNow.ToString("O"));
var r2 = await httpClient.SendAsync(binaryRequest);
var j2 = JsonSerializer.Deserialize<JsonElement>(await r2.Content.ReadAsStringAsync());
Console.WriteLine($"[binary]     status={r2.StatusCode} is_error={j2.GetProperty("is_error")}");

Console.WriteLine("\nBoth content modes accepted.");

// Expected output:
// Sending in structured mode:
// [structured] status=Accepted is_error=false
//
// Sending in binary mode:
// [binary]     status=Accepted is_error=false
//
// Both content modes accepted.
