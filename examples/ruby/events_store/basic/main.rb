# Example: events_store/basic
# Subscribes with StartNewOnly (events_store_type=1), publishes one event, receives it.
require "net/http"; require "uri"; require "json"; require "timeout"; require "cloud_events"; require "securerandom"
def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")

base = server_url; channel = "ruby-ce-events-store.basic"
sdk = CloudEvents::HttpBinding.default
received = Queue.new

subscriber = Thread.new do
  uri = URI("#{base}/ce/subscribe/events-store?client_id=ruby-es-sub&channel=#{URI.encode_www_form_component(channel)}&events_store_type=1")
  Net::HTTP.start(uri.host, uri.port) do |http|
    req = Net::HTTP::Get.new(uri); req["Accept"] = "text/event-stream"
    http.request(req) do |resp|
      ev_type = nil; data = nil
      resp.read_body do |chunk|
        chunk.each_line do |line|
          line.chomp!
          if line.empty?
            if ev_type == "cloudevent" && data
              received.push(data); Thread.exit
            elsif ev_type == "error"
              $stderr.puts "SSE error: #{data}"; Thread.exit
            end
            ev_type = nil; data = nil
          elsif line.start_with?(":") then # keepalive
          elsif line.start_with?("event:") then ev_type = line.sub("event:", "").strip
          elsif line.start_with?("data:") then data = line.sub("data:", "").strip
          end
        end
      end
    end
  end
end

sleep 0.5

event = CloudEvents::Event::V1.new(
  id: SecureRandom.uuid, type: "com.kubemq.examples.eventsstore.stored",
  source: URI("urn:kubemq-ce-ruby-example"), subject: channel,
  spec_version: "1.0",
  data_content_type: CloudEvents::ContentType.new("application/json"),
  data: JSON.generate({ msg: "hello events-store from Ruby!" })
)
enc_h, enc_b = sdk.encode_event(event, structured_format: "json")
uri = URI("#{base}/ce/send/event-store")
Net::HTTP.start(uri.host, uri.port) do |http|
  req = Net::HTTP::Post.new(uri); enc_h.each{|k,v|req[k]=v}; req.body = enc_b
  res = http.request(req)
  puts "Published to events-store: status=#{res.code}"
end

data = Timeout.timeout(10) { received.pop }
ce = JSON.parse(data)
puts "Received: type=#{ce['type']} data=#{ce['data']}"
subscriber.join(1)

# Expected output:
# Published to events-store: status=202
# Received: type=com.kubemq.examples.eventsstore.stored data={"msg"=>"hello events-store from Ruby!"}
