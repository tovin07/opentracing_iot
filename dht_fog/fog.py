import sys
import time
import json

import opentracing
import zipkin_ot.tracer

from myconfig import *

def handle_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        global Connected
        Connected = True
    else:
        print("Connection failed")

Connected = False

def handle_message(client, userdata, message):
    print("message recieve ", str(message.payload.decode("utf-8")))
    print("message topic ", message.topic)
    print("message qos ", message.qos)
    print("message retain flag ", message.retain)


# import mqtt
import paho.mqtt.client as mqtt

mqttConfig = MqttConfig()

client=mqtt.Client()
client.username_pw_set(mqttConfig.user, mqttConfig.passworld)

# handle connection and message
client.on_connect = handle_connect
client.on_message = handle_message
client.connect(mqttConfig.broker_address, mqttConfig.port)


while True:
    client.loop_start()
    client.subscribe(mqttConfig.clientName + '/Temperature')
    time.sleep(1)

