#!/usr/bin/env ruby

require 'mqtt'
require 'json'
require 'kafka'
require 'base64'

$stdout.sync = true
@name = "headcountsm2k"
kafkah = {}
kafkah["broker"] = ENV['KAFKA_BROKER'].nil? ? "127.0.0.1" : ENV['KAFKA_BROKER']
kafkah["port"] = ENV['KAFKA_PORT'].nil? ? "9092" : ENV['KAFKA_PORT']
kafkah["topic"] = ENV['KAFKA_TOPIC'].nil? ? "ubicquia" : ENV['KAFKA_TOPIC']
mqtth = {}
mqtth["broker"] = ENV['MQTT_BROKER'].nil? ? "127.0.0.1" : ENV['MQTT_BROKER']
mqtth["port"] = ENV['MQTT_PORT'].nil? ? "1883" : ENV['MQTT_PORT']
mqtth["topic"] = ENV['MQTT_TOPIC'].nil? ? "headcounts" : ENV['MQTT_TOPIC']
mqtth["username"] = ENV['MQTT_USERNAME'] unless ENV['MQTT_USERNAME'].nil?
mqtth["password"] = ENV['MQTT_PASSWORD'] unless ENV['MQTT_PASSWORD'].nil?

def j2j(event)
    event
end

def m2k(mqtth,kafkah)
    begin
        puts "Connecting to mqtt server #{mqttc.host}"
        kclient = Kafka.new(seed_brokers: ["#{kafka_broker}:#{kafka_port}"], client_id: @name)
        mqttc = MQTT::Client.new
        mqttc.host = mqtth["host"]
        mqttc.port = mqtth["port"]
        mqttc.username = mqtth["username"] unless mqtth["username"].nil?
        mqttc.password = mqtth["password"] unless mqtth["password"].nil?
        mqttc.connect
        mqttc.subscribe(mqtth["topic"])
        while true
            topic,message = mqttc.get
            m = j2j(JSON.parse(message))
            puts "time: #{Time.now}, message: #{m.to_json}"
            kclient.deliver_message("#{m.to_json}",topic: kafkah["topic"])
        end
    rescue Exception => e
        puts "Exception: #{e.class}, message: #{e.message}"
        puts "Disconnecting from brokers"
        mqttc.disconnect
        kclient.shutdown
        puts "[#{@name}] Stopping ubicquia2k poc thread"
    end 
end

Signal.trap('INT') { throw :sigint }

catch :sigint do
    while true
        puts "[#{@name}] Starting headcountsm2k thread"
        t1 = Thread.new{m2k(mqtth,kafkah)}
        t1.join
    end
end

puts "Exiting from headcountsm2k"

## vim:ts=4:sw=4:expandtab:ai:nowrap:formatoptions=croqln:
