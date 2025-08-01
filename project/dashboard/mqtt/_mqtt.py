import random
import json


from dash_back.models import Post, Online #type: ignore
from paho.mqtt import client as mqtt_client #type: ignore
from datetime import datetime, timezone

broker = '159.89.103.242'
port = 1883
topic1 = "data/#"

topic2 = "mqtt/mqtt"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
# username = 'emqx'
# password = 'public'

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    #client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        #print(f"Received `{msg.payload.payload}` from `{msg.topic}` topic")
        #data = msg.payload.decode()
        topic = msg.topic
        myList = topic.split('/')
        dev_id = myList[1]
        data_out=json.loads(msg.payload.decode())
        timestamp = int(data_out['payload']['timestamp'])
        timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

        value = float(data_out['payload']['power'])
        print(data_out,dev_id)
        Post.objects.get_or_create(devId=dev_id,value=value, created_date=timestamp)
        Online.objects.get_or_create(dev=dev_id, saved_date=timestamp, pow=value )
    client.subscribe([(topic1, 0), (topic2, 0)])
    client.on_message = on_message

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_start()

if __name__ == '__main__':
    run()
