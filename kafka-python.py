from confluent_kafka import Producer, Consumer, KafkaError

# Configurações do produtor
producer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092',
    'client.id': 'python-producer'
}

# Configurações do consumidor
consumer_config = {
    'bootstrap.servers': 'localhost:29092,localhost:39092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Criar produtor
producer = Producer(**producer_config)

# Função de callback para confirmação de entrega
def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Enviar mensagem
topic = 'test_topic'
message = 'Hello, Kafka!'
producer.produce(topic, message.encode('utf-8'), callback=delivery_callback)

# Aguardar a entrega de todas as mensagens
producer.flush()

# Criar consumidor
consumer = Consumer(**consumer_config)

# Assinar tópico
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