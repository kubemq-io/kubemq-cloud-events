# Example: routing/cesql_routing — publish events for server-side CESQL routing.
# Requires KubeMQ configured with CESQL rules (see Go example for config).
require "net/http"; require "uri"; require "json"; require "timeout"; require "securerandom"; require "cloud_events"
def server_url = ENV.fetch("KUBEMQ_CE_URL", "http://localhost:9090")

base = server_url; sdk = CloudEvents::HttpBinding.default
puts "CESQL Routing Example — Ruby\nRequires KubeMQ with CESQL routing configured.\n"

results = Queue.new

[["order-archive","ruby-order-sub",1],["alert-stream","ruby-alert-sub",1],["all-events","ruby-all-sub",3]].each do |ch, cid, max|
  Thread.new do
    uri = URI("#{base}/ce/subscribe/events?client_id=#{cid}&channel=#{URI.encode_www_form_component(ch)}")
    count = 0
    Net::HTTP.start(uri.host, uri.port) do |http|
      req = Net::HTTP::Get.new(uri); req["Accept"] = "text/event-stream"
      http.request(req) do |resp|
        ev_type = nil; data = nil
        resp.read_body do |chunk|
          chunk.each_line do |line|
            line.chomp!
            if line.empty?
              if ev_type == "cloudevent" && data
                ce = JSON.parse(data)
                results.push("  [#{ch}] type=#{ce['type']}")
                count += 1
                Thread.exit if count >= max
              end
              ev_type = nil; data = nil
            elsif line.start_with?("event:") then ev_type = line.sub("event:","").strip
            elsif line.start_with?("data:") then data = line.sub("data:","").strip
            end
          end
        end
      end
    end
  end
end

sleep 0.5

%w[com.kubemq.examples.routing.order com.kubemq.examples.routing.alert com.kubemq.examples.routing.info].each do |ev_type|
  ev = CloudEvents::Event::V1.new(
    id: SecureRandom.uuid, type: ev_type,
    source: URI("urn:kubemq-ce-ruby-cesql"), subject: "routing-source",
    spec_version: "1.0",
    data_content_type: CloudEvents::ContentType.new("application/json"),
    data: JSON.generate({ description: "Event of type #{ev_type}" }))
  enc_h, enc_b = sdk.encode_event(ev, structured_format: "json")
  uri = URI("#{base}/ce/send/event")
  Net::HTTP.start(uri.host, uri.port) do |http|
    req = Net::HTTP::Post.new(uri); enc_h.each{|k,v|req[k]=v}; req.body=enc_b
    res = http.request(req)
    puts "Published type=#{ev_type} (status=#{res.code})"
  end
end

sleep 2
5.times { r = begin; Timeout.timeout(0.1) { results.pop }; rescue Timeout::Error; nil; end; puts r if r }
puts "\nCESQL routing demonstration complete."
