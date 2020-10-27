from kafka import KafkaConsumer

consumer = KafkaConsumer('age_result')
for msg in consumer:
    print((msg.value).decode('utf8'))