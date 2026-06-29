# Example: events/basic_pubsub
#
# Demonstrates basic fire-and-forget event pub/sub via the KubeMQ
# CloudEvents HTTP connector.
#
# Run: ruby events/basic_pubsub/main.rb
require "net/http"
require "uri"
require "json"
require "timeout"
require "securerandom"
require "cloud_events"

def server_url
  ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")
end

base    = server_url
channel = "ruby-ce-events.basic-pubsub"
client_id = "kubemq-ce-ruby-example"

received = Queue.new

# SSE subscriber thread.
subscriber = Thread.new do
  uri = URI("#{base}/ce/subscribe/events?client_id=#{client_id}-sub&channel=#{URI.encode_www_form_component(channel)}")
  Net::HTTP.start(uri.host, uri.port) do |http|
    req = Net::HTTP::Get.new(uri)
    req["Accept"] = "text/event-stream"
    req["Cache-Control"] = "no-cache"
    http.request(req) do |resp|
      ev_type = nil
      data    = nil
      resp.read_body do |chunk|
        chunk.each_line do |line|
          line.chomp!
          if line.empty?
            if ev_type == "cloudevent" && data
              received.push(data)
              Thread.exit
            end
            ev_type = nil
            data    = nil
          elsif line.start_with?(":") # keepalive
          elsif line.start_with?("event:")
            ev_type = line.sub("event:", "").strip
          elsif line.start_with?("data:")
            data = line.sub("data:", "").strip
          end
        end
      end
    end
  end
end

# Allow subscription to establish.
sleep 0.5

# Build and publish CloudEvent (structured mode).
sdk  = CloudEvents::HttpBinding.default
event = CloudEvents::Event::V1.new(
  id:              SecureRandom.uuid,
  type:            "com.kubemq.examples.events.sent",
  source:          URI("urn:#{client_id}"),
  subject:         channel,
  spec_version:    "1.0",
  data_content_type: CloudEvents::ContentType.new("application/json"),
  data:            JSON.generate({ message: "Hello from Ruby CloudEvents example!" })
)

# Encode as structured mode.
headers, body = sdk.encode_event(event, structured_format: "json")
uri = URI("#{base}/ce/send/event")
Net::HTTP.start(uri.host, uri.port) do |http|
  req = Net::HTTP::Post.new(uri)
  req["Content-Type"] = headers["Content-Type"]
  req.body = body
  res = http.request(req)
  result = JSON.parse(res.body)
  puts "Published: status=#{res.code} is_error=#{result['is_error']}"
end

# Wait for subscriber.
data = nil
Timeout.timeout(10) { data = received.pop }
ce = JSON.parse(data)
puts "Received event:"
puts "  type:    #{ce['type']}"
puts "  source:  #{ce['source']}"
puts "  subject: #{ce['subject']}"
puts "  data:    #{ce['data']}"

# Expected output:
# Published: status=202 is_error=false
# Received event:
#   type:    com.kubemq.examples.events.sent
#   source:  urn:kubemq-ce-ruby-example
#   subject: ruby-ce-events.basic-pubsub
#   data:    {"message"=>"Hello from Ruby CloudEvents example!"}
