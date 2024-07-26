from faker import Faker
from kafka import KafkaProducer
import json

fake = Faker()

data = []
for i in range(0, 100):
    row = {
        "device_id": fake.bothify(text='D02X01M###'),
        "sensor_id": fake.bothify(text='sensor-####'),
        "location": fake.city(),
        "temperature": str(fake.random_number(digits=2)) + "." + str(fake.random_number(digits=1)),
        "humidity": str(fake.random_number(digits=2)) + "." + str(fake.random_number(digits=1)),
        "timestamp": fake.iso8601()[:21]
    }
    data.append(row)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send("topic", value={"data": data})
producer.flush()