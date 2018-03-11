import sys
import time
import json

import paho.mqtt.client as mqtt

Connected = False

def handle_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        global Connected
        Connected = True
    else:
        print("Connection failed")

broker_address="XXXXXXXX"
port = XXXXXX
user =  "XXXX"
passworld = "XXXXXX"

client = mqtt.Client()
client.username_pw_set(user, passworld)
client.on_connect = handle_connect
client.connect(broker_address, port)

while True:
    client.loop_start()
    client.publish("test", "Hello")
    time.sleep(5)
   
