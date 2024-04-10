from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import os

load_dotenv()



# Configurações do consumidor
consumer_config = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'group.id': os.getenv('CLUSTER_ID'),
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms':   "PLAIN",
	'sasl.username':     os.getenv("API_KEY"),
	'sasl.password':     os.getenv("API_SECRET")
}

# Criar consumidor
consumer = Consumer(**consumer_config)

# Assinar tópico
topic = 'teste'
consumer.subscribe([topic])

# Consumir mensagens
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f'Received message: {msg.value().decode("utf-8")}')
except KeyboardInterrupt:
    pass
finally:
    # Fechar consumidor
    consumer.close()
