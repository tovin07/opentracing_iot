from influxdb import InfluxDBClient

import time
# import ast
import json

#  from matplotlib.font_manager import json_dump

# opentracing
import opentracing
import zipkin_ot.tracer

from myconfig import *
zipkinConfig = ZipkinConfig()

# mqtt
import paho.mqtt.client as mqtt


def handle_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        global isConnectedBroker
        isConnectedBroker = True
    else:
        print("Connection failed")


def write_db(data_json):
    print("Write to database")
    data_write_db = []

    for key, value in data_json.items():
        record = {
            'measurement': 'weather',
            'tags': {
                'thing_type': key
            },
            'fields': {
                'value': value
            }
        }
        data_write_db.append(record)

    clientDB.write_points(data_write_db)
    print('Updated Database')


def handle_message(client, userdata, message):
    if (message.topic == mqttConfig.clientName + '/DbWriterTextCarrier'):
        print("message recieve text carrier ", (message.payload))
        try:
            global recevieTextCarrier
            global text_carrier_recive
            recevieTextCarrier = True
            text_carrier_recive = json.loads(message.payload)
        except TypeError:
            print("Type error")

    if (message.topic == mqttConfig.clientName + '/DbWriter'):
        global receiveData
        receiveData = True
        print("message recieve text carrier ",
        str(message.payload.decode("utf-8")))
        global json_data
        json_data = message.payload
        write_db(json.loads(message.payload))


isConnectedBroker = False
recevieTextCarrier = False
receiveData = False
text_carrier_recive = None
json_data = None


mqttConfig = MqttConfig()
clientMQTT = mqtt.Client()
# clientMQTT.username_pw_set(mqttConfig.user, mqttConfig.passworld)

clientMQTT.on_connect = handle_connect
clientMQTT.on_message = handle_message
clientMQTT.connect(mqttConfig.broker_address,
                   mqttConfig.port)  # connect to broker

clientDB = InfluxDBClient('localhost', 8086, 'root', 'root', 'testInfluxDB')
clientDB.create_database('testInfluxDB')

if __name__ == '__main__':
    with zipkin_ot.Tracer(
            service_name = 'DHT_PI',
            collector_host = zipkinConfig.host,
            collector_port = zipkinConfig.port,
            verbosity = 1
            ) as tracer:
        opentracing.tracer = tracer

    while True:
        clientMQTT.loop_start()
        clientMQTT.subscribe(mqttConfig.clientName + '/DbWriterTextCarrier')
        
        if (text_carrier_recive and recevieTextCarrier):    
            recevieTextCarrier = False
            span_context_recive = opentracing.tracer.extract(opentracing.Format.TEXT_MAP, text_carrier_recive)

            with opentracing.tracer.start_span('db_writter_span', child_of=span_context_recive) as db_writter_span:
                clientMQTT.subscribe(mqttConfig.clientName + '/DbWriter')
                if (receiveData):
                    db_writter_span.log_event('data write to database', payload=json_data)      
        
        time.sleep(1)

# clientMQTT.subscribe('dbwriter/request/api_write_db')
# clientMQTT.message_callback_add('dbwriter/request/api_write_db', api_write_db)
