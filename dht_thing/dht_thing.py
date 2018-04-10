import sys
# import Adafruit_DHT
import time
import json
import random

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
            root_span.set_tag('url', 'localhost')

            text_carrier = {}
            opentracing.tracer.inject(root_span.context, opentracing.Format.TEXT_MAP, text_carrier)
            
            with opentracing.start_child_span(root_span, 'get sensor data') as get_dt11_data_span:
                # humidity, temperature = Adafruit_DHT.read_retry(11, 4) # DHT 11 Pin 4
                humidity = random.randint(60, 99)
                temperature = random.randint(10, 30)   
                get_dt11_data_span.log_event('humidity', payload=humidity)
                get_dt11_data_span.log_event('temperature', payload=temperature)
            # with opentracing.tracer.start_span('parse_data_span' , child_of=span_context) as parse_data_span:
            #     data = {'humidity': humidity, 'temperature': temperature}
            #     json_data = json.dumps(data)

            with opentracing.start_child_span(root_span , 'encode data') as encode_data_span:
                data = {'humidity': humidity, 'temperature': temperature}
                with opentracing.start_child_span(encode_data_span, 'validate data'):
                    try:
                        json_data = json.dumps(data)
                        encode_data_span.log_event('weather data', payload=json_data)
                    except TypeError:
                        print("Unable to encode the object")
                        json_data = {'humidity': 'Unable to encode the object', 'temperature': 'Unable to encode the object'}
                        
            with opentracing.start_child_span(root_span, 'send data') as send_to_fog_span:
                client.loop_start()
                client.publish(mqttConfig.clientName + '/TextCarrier', json.dumps(text_carrier))
                client.publish(mqttConfig.clientName + '/Temperature', json_data)
                #client.loop_stop()
            # root_span.finish();

        # print 'Temp: {0:0.1f} C Humidity: {1:0.1f} %'.format(temperature, humidity)
        time.sleep(10)
