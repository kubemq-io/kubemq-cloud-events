# Example: queues/ack_all — drain queue with /ce/queue/ack_all.
require "net/http"; require "uri"; require "json"; require "securerandom"; require "cloud_events"
def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")
base = server_url; channel = "ruby-ce-queues.ack-all"; client_id = "kubemq-ce-ruby-example"
sdk = CloudEvents::HttpBinding.default

(1..5).each do |i|
  ev = CloudEvents::Event::V1.new(id: SecureRandom.uuid, type: "com.kubemq.examples.queues.ackall",
    source: URI("urn:#{client_id}"), subject: channel, spec_version: "1.0",
    data_content_type: CloudEvents::ContentType.new("application/json"),
    data: JSON.generate({ n: i }))
  enc_h, enc_b = sdk.encode_event(ev, structured_format: "json")
  uri = URI("#{base}/ce/queue/send")
  Net::HTTP.start(uri.host, uri.port) { |http| req = Net::HTTP::Post.new(uri); enc_h.each{|k,v|req[k]=v}; req.body=enc_b; http.request(req) }
end
puts "Sent 5 messages."

def peek_count(base, channel, client_id)
  uri = URI("#{base}/ce/queue/receive")
  uri.query = URI.encode_www_form(channel: channel, client_id: client_id, max_messages: 100, wait_timeout: 3, is_peek: "true")
  Net::HTTP.start(uri.host, uri.port) { |http| JSON.parse(http.request(Net::HTTP::Post.new(uri)).body).dig('data','messages_received') || 0 }
end

puts "Peek: #{peek_count(base, channel, client_id)} messages in queue."
uri = URI("#{base}/ce/queue/ack_all")
uri.query = URI.encode_www_form(channel: channel, client_id: client_id, wait_timeout: 5)
Net::HTTP.start(uri.host, uri.port) do |http|
  r = JSON.parse(http.request(Net::HTTP::Post.new(uri)).body)
  puts "ack_all: is_error=#{r['is_error']} message=#{r['message']}"
end
puts "After ack_all: #{peek_count(base, channel, client_id)} messages remaining."
