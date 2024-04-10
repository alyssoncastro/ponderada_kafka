#bibliotecas
import os
import ssl
import paho.mqtt.client as mqtt
import time
import json
import random
from dotenv import load_dotenv

load_dotenv()

broker_address = os.getenv("BROKER_HIVE")
port = int(os.getenv("PORT_HIVE"))
username = os.getenv("HIVE_USER")
password = os.getenv("HIVE_PASS")


def on_connect(client, userdata, flags, rc):
    print(f"Conectado com código de resultado: {rc}")



qos_level = 0
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "python_publisher")
client.on_connect = on_connect



# Configurações de TLS e autenticação
client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)
client.username_pw_set(username, password)


# Conecta ao broker MQTT
client.connect(broker_address, port, 60)


#dados
sensorDATA = {
    "detectable_gases": [
        {
        "gas": "Carbon Monoxide (CO)",
        "symbol": "CO",
        "detection_range": "1 - 1000 ppm",
        "sensitivity": "Indicative, Rs at 60 ppm CO",
        "test_conditions": "23 ± 5°C, 50 ± 10% RH"
        },
        {
        "gas": "Nitrogen Dioxide (NO2)",
        "symbol": "NO2",
        "detection_range": "0.05 - 10 ppm",
        "sensitivity": "Indicative, Rs at 0.25 ppm NO2",
        "test_conditions": "23 ± 5°C, ≤ 5% RH"
        },
        {
        "gas": "Ethanol",
        "symbol": "C2H5OH",
        "detection_range": "10 - 500 ppm",
        "sensitivity": "Not specified",
        "test_conditions": "Not specified"
        },
        {
        "gas": "Hydrogen (H2)",
        "symbol": "H2",
        "detection_range": "1 - 1000 ppm",
        "sensitivity": "Indicative, Rs at 1 ppm H2",
        "test_conditions": "23 ± 5°C, 50 ± 10% RH"
        },
        {
        "gas": "Ammonia (NH3)",
        "symbol": "NH3",
        "detection_range": "1 - 500 ppm",
        "sensitivity": "Indicative, Rs at 1 ppm NH3",
        "test_conditions": "23 ± 5°C, 50 ± 10% RH"
        },
        {
        "gas": "Methane (CH4)",
        "symbol": "CH4",
        "detection_range": ">1000 ppm",
        "sensitivity": "Not specified",
        "test_conditions": "Not specified"
        },
        {
        "gas": "Propane (C3H8)",
        "symbol": "C3H8",
        "detection_range": ">1000 ppm",
        "sensitivity": "Not specified",
        "test_conditions": "Not specified"
        },
        {
        "gas": "Iso-butane (C4H10)",
        "symbol": "C4H10",
        "detection_range": ">1000 ppm",
        "sensitivity": "Not specified",
        "test_conditions": "Not specified"
        }
    ]
}



# Loop para publicar mensagens continuamente
try:
    while True:
        READING_SIMULATED = {
        "CO": round(random.uniform(1, 1000), 2),
        "NO2": round(random.uniform(0.05, 10), 2),
        "C2H5OH": round(random.uniform(10, 500), 2),
        "H2": round(random.uniform(1, 1000), 2),
        "NH3": round(random.uniform(1, 500), 2),
        "CH4": round(random.uniform(1000, 2000), 2),
        "C3H8": round(random.uniform(1000, 2000), 2),
        "C4H10": round(random.uniform(1000, 2000), 2)
        }

       
        dataPUBLISH = {
            "sensor": sensorDATA,
            "reading": READING_SIMULATED
        }
        message = json.dumps(dataPUBLISH)
        client.publish("sensor/gases", message, qos=qos_level)
        print(f"Published:{message} with QoS  {qos_level}")
        time.sleep(4)


except KeyboardInterrupt:
    print("Publicação encerrada")
    client.disconnect()