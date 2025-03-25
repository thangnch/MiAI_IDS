import random
from kafka import KafkaProducer
import time

# Cấu hình Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Địa chỉ Kafka broker
    value_serializer=lambda v: v.encode('utf-8')
)

# Tên topic Kafka
topic_name = 'network_attack'

# Get randome test
with open("KDDTest+.txt") as f:
    lines = f.readlines()

lines = random.sample(lines, 10)
print(len(lines))

# Gửi tin nhắn
for i in lines:
    message = i

    # Gửi tin nhắn vào Kafka
    producer.send(topic_name, message)
    print(f"Đã gửi tin nhắn: {message}")
    time.sleep(random.randint(1,2))  # Random 1,2 giây

# Đóng producer khi hoàn tất
producer.close()
