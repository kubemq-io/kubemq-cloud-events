# Example: queues/peek — is_peek=true inspection.
require "net/http"; require "uri"; require "json"; require "securerandom"; require "cloud_events"
def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")
base = server_url; channel = "ruby-ce-queues.peek"; sdk = CloudEvents::HttpBinding.default

(1..3).each do |i|
  ev = CloudEvents::Event::V1.new(id: SecureRandom.uuid, type: "com.kubemq.examples.queues.peek",
    source: URI("urn:kubemq-ce-ruby-example"), subject: channel, spec_version: "1.0",
    data_content_type: CloudEvents::ContentType.new("application/json"),
    data: JSON.generate({ n: i }))
  enc_h, enc_b = sdk.encode_event(ev, structured_format: "json")
  uri = URI("#{base}/ce/queue/send")
  Net::HTTP.start(uri.host, uri.port) { |http| req = Net::HTTP::Post.new(uri); enc_h.each { |k,v| req[k]=v }; req.body=enc_b; http.request(req) }
end
puts "Sent 3 messages.\n"

def peek_or_receive(base, channel, is_peek, label)
  uri = URI("#{base}/ce/queue/receive")
  uri.query = URI.encode_www_form(channel: channel, client_id: "ruby-peek-client",
                                   max_messages: 10, wait_timeout: 3, is_peek: is_peek.to_s)
  Net::HTTP.start(uri.host, uri.port) do |http|
    res = http.request(Net::HTTP::Post.new(uri))
    n = JSON.parse(res.body).dig('data', 'messages_received') || 0
    puts "[#{label}] messages_received=#{n}"
    n
  end
end

peek_or_receive(base, channel, true, "peek #1")
peek_or_receive(base, channel, true, "peek #2")
peek_or_receive(base, channel, false, "consume")
