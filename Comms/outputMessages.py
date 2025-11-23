import paho.mqtt.client as mqtt
import time

BROKER = "localhost"
PORT = 1883
TOPIC = "topic/hello"

# ----------------------------
# Callback when connected
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        # Subscribe immediately after connecting
        client.subscribe(TOPIC)
    else:
        print(f"Failed to connect. Return code {rc}")

# ----------------------------
# Callback when a message is received
def on_message(client, userdata, msg):
    print(f"Received message: {msg.payload.decode()} on topic {msg.topic}")

# ----------------------------
# Create MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to broker
client.connect(BROKER, PORT, 60)
client.loop_start()  # start background network loop

# ----------------------------
# Publish messages in a loop
try:
    count = 0
    while True:
        message = f"Hello from Python! #{count}"
        client.publish(TOPIC, message)
        print(f"Sent: {message}")
        count += 1
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping...")
finally:
    client.loop_stop()
    client.disconnect()
