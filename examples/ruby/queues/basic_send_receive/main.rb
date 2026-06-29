# Example: queues/basic_send_receive — send 3 queue messages, receive one at a time.
require "net/http"
require "uri"
require "json"
require "securerandom"
require "cloud_events"

def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")

base      = server_url
channel   = "ruby-ce-queues.basic"
client_id = "kubemq-ce-ruby-worker"
sdk       = CloudEvents::HttpBinding.default

puts "Sending 3 messages to queue '#{channel}':"
(1..3).each do |i|
  event = CloudEvents::Event::V1.new(
    id: SecureRandom.uuid, type: "com.kubemq.examples.queues.task",
    source: URI("urn:kubemq-ce-ruby-example"), subject: channel,
    spec_version: "1.0",
    data_content_type: CloudEvents::ContentType.new("application/json"),
    data: JSON.generate({ task_id: i, task: "process-item" })
  )
  enc_headers, enc_body = sdk.encode_event(event, structured_format: "json")
  uri = URI("#{base}/ce/queue/send")
  Net::HTTP.start(uri.host, uri.port) do |http|
    req = Net::HTTP::Post.new(uri)
    enc_headers.each { |k, v| req[k] = v }
    req.body = enc_body
    res = http.request(req)
    r = JSON.parse(res.body)
    puts "  Sent task #{i}: status=#{res.code} is_error=#{r['is_error']}"
  end
end

puts "\nReceiving 3 messages:"
3.times do
  uri = URI("#{base}/ce/queue/receive")
  uri.query = URI.encode_www_form(channel: channel, client_id: client_id,
                                   max_messages: 1, wait_timeout: 5)
  Net::HTTP.start(uri.host, uri.port) do |http|
    res = http.request(Net::HTTP::Post.new(uri))
    r = JSON.parse(res.body)
    next puts "  Error: #{r['message']}" if r['is_error']
    msgs = r.dig('data', 'messages') || []
    msgs.empty? ? puts("  No messages") : msgs.each { |m| puts "  Received: type=#{m['type']} data=#{m['data']}" }
  end
end
