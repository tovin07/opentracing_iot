import sys
import Adafruit_DHT
import time
import json

import opentracing
import zipkin_ot.tracer

from myconfig import *

zipkinConfig = ZipkinConfig()

# import mqtt
import paho.mqtt.client as mqtt

def handle_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        global Connected
        Connected = True
    else:
        print("Connection failed")


# mqttt config
mqttConfig = MqttConfig()
client=mqtt.Client()

client.username_pw_set(mqttConfig.user, mqttConfig.passworld)
client.on_connect = handle_connect

client.connect(mqttConfig.broker_address, mqttConfig.port)

Connected = False

if __name__ == '__main__':
    with zipkin_ot.Tracer(
            service_name = 'DHT_PI',
            collector_host = zipkinConfig.host,
            collector_port = zipkinConfig.port,
            verbosity = 1
            ) as tracer:
        opentracing.tracer = tracer

    while True:
        with opentracing.tracer.start_span('DHT_PI_ROOT') as root_span:
            text_carrier = {}
            
            # inject text_carrier and send it to server
            # opentracing.tracer.inject(root_span.context, opentracing.Format.TEXT_MAP, text_carrier)

            with opentracing.start_child_span(root_span, 'Get data from sensor') as get_dt11_data_span:
                humidity, temperature = Adafruit_DHT.read_retry(11, 4) # DHT 11 Pin 4
                with opentracing.start_child_span(get_dt11_data_span, 'parse data') as parse_data_span:
                    data = {'humidity': humidity, 'temperature': temperature}
                    json_data = json.dumps(data)
                    with opentracing.start_child_span(parse_data_span, 'send data to fog through mqtt') as send_to_fog_span:
                        client.loop_start()
                        client.publish(mqttConfig.clientName + '/Temperature', json_data)
                        #client.loop_stop()
        root_span.finish();

        print 'Temp: {0:0.1f} C Humidity: {1:0.1f} %'.format(temperature, humidity)
        time.sleep(5)
