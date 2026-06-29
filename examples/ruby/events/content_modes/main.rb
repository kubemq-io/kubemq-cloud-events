# Example: events/content_modes — structured and binary mode.
require "net/http"
require "uri"
require "json"
require "time"
require "securerandom"
require "cloud_events"

def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")

base = server_url
sdk  = CloudEvents::HttpBinding.default

event = CloudEvents::Event::V1.new(
  id:           SecureRandom.uuid,
  type:         "com.kubemq.examples.events.content-mode",
  source:       URI("urn:kubemq-ce-ruby-example"),
  subject:      "ruby-ce-events.content-modes",
  spec_version: "1.0",
  data_content_type: CloudEvents::ContentType.new("application/json"),
  data: JSON.generate({ message: "hello content modes" })
)

def post_event(base, headers_hash, body)
  uri = URI("#{base}/ce/send/event")
  Net::HTTP.start(uri.host, uri.port) do |http|
    req = Net::HTTP::Post.new(uri)
    headers_hash.each { |k, v| req[k] = v }
    req.body = body
    res = http.request(req)
    JSON.parse(res.body).merge("status" => res.code)
  end
end

# Structured mode
puts "Sending in structured mode:"
enc_headers, enc_body = sdk.encode_event(event, structured_format: "json")
r1 = post_event(base, enc_headers, enc_body)
puts "[structured] status=#{r1['status']} is_error=#{r1['is_error']}"

# Binary mode — CE attrs in headers, data as body
puts "\nSending in binary mode:"
binary_headers = {
  "Content-Type"   => "application/json",
  "ce-specversion" => "1.0",
  "ce-type"        => event.type,
  "ce-source"      => event.source.to_s,
  "ce-id"          => event.id,
  "ce-subject"     => event.subject,
  "ce-time"        => Time.now.utc.iso8601(9),
}
r2 = post_event(base, binary_headers, event.data)
puts "[binary]     status=#{r2['status']} is_error=#{r2['is_error']}"

puts "\nBoth content modes accepted."
