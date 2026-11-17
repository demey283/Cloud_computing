"""Eenvoudige MQTT-client: abonneert op een topic en publiceert elke 5 s een bericht."""
import json
import os
import time
from datetime import datetime

import paho.mqtt.client as mqtt

BROKER = os.getenv("MQTT_HOST", "mqtt5")
PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "project/test")


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Verbonden met {BROKER}:{PORT} (code {reason_code})")
    client.subscribe(TOPIC)


def on_message(client, userdata, msg):
    print(f"Ontvangen op {msg.topic}: {msg.payload.decode()}")


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="python-app")
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(BROKER, PORT, keepalive=60)
        break
    except OSError as e:
        print(f"Broker nog niet bereikbaar ({e}), opnieuw over 2 s...")
        time.sleep(2)

client.loop_start()
teller = 0
while True:
    teller += 1
    bericht = {"teller": teller, "tijd": datetime.now().isoformat(timespec="seconds")}
    client.publish(TOPIC, json.dumps(bericht))
    time.sleep(5)
