import sys
# import Adafruit_DHT
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
        global isConnectedBroker
        isConnectedBroker = True
    else:
        print("Connection failed")



def handle_message(client, userdata, message):
    if (message.topic == mqttConfig.clientName + '/TextCarrier'):
        print("message recieve text carrier ", (message.payload))
        try:
            global recevieTextCarrier
            global text_carrier_recive
            recevieTextCarrier = True
            text_carrier_recive = json.loads(message.payload)
        except TypeError:
            print("Type error")

    if (message.topic == mqttConfig.clientName + '/Temperature'):
        global receiveData 
        receiveData = True
        print("message recieve text carrier ", str(message.payload.decode("utf-8")))
        global json_data 
        json_data = message.payload

isConnectedBroker = False
recevieTextCarrier =False
receiveData = False
text_carrier_recive = None

mqttConfig = MqttConfig()

client=mqtt.Client()
# client.username_pw_set(mqttConfig.user, mqttConfig.passworld)

# handle connection and message
client.on_connect = handle_connect
client.on_message = handle_message
client.connect(mqttConfig.broker_address, mqttConfig.port)

if __name__ == '__main__':
    with zipkin_ot.Tracer(
            service_name = 'DHT_PI',
            collector_host = zipkinConfig.host,
            collector_port = zipkinConfig.port,
            verbosity = 1
            ) as tracer:
        opentracing.tracer = tracer

    while True:
        client.loop_start()
        client.subscribe(mqttConfig.clientName + '/TextCarrier')

        if(text_carrier_recive and recevieTextCarrier):
            print('create span context')
            recevieTextCarrier = False
            span_context_recive = opentracing.tracer.extract(opentracing.Format.TEXT_MAP, text_carrier_recive)
            with opentracing.tracer.start_span('fog level', child_of=span_context_recive) as fog_span:
                text_carrier_send = {}
                opentracing.tracer.inject(fog_span.context, opentracing.Format.TEXT_MAP, text_carrier_send)
                with opentracing.start_child_span(fog_span, 'subscribe') as subscribe_span:
                    client.subscribe(mqttConfig.clientName + '/Temperature')
                    if (receiveData):
                        subscribe_span.log_event('data recive from thing level', payload=json_data)
                with opentracing.start_child_span(fog_span, 'publish') as publish_span:
                    if (receiveData):
                        client.publish(mqttConfig.clientName + '/FilterTextCarrier', json.dumps(text_carrier_send))
                        client.publish(mqttConfig.clientName + '/Filter', json_data)
                        publish_span.log_event('data send to filer', payload=json_data)
        time.sleep(0.5)

