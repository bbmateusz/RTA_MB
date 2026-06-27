from kafka import KafkaProducer
from datetime import datetime
import json, time, random

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

print("Producer wystartował. Wysyłam transakcje na temat 'transactions'... (Ctrl+C aby przerwać)")

i = 0
while True:
    # co jakiś czas wygeneruj podejrzaną transakcję
    if random.random() < 0.2:
        tx = {
            "amount": round(random.uniform(2000, 9000), 2),
            "is_electronics": 1,
            "tx_per_minute": random.randint(8, 15)
        }
    else:
        tx = {
            "amount": round(random.uniform(5, 500), 2),
            "is_electronics": random.randint(0, 1),
            "tx_per_minute": random.randint(1, 5)
        }

    tx["timestamp"] = datetime.now().isoformat()

    producer.send('transactions', tx)
    producer.flush()
    i += 1
    print(f"[{i}] Wysłano: {tx}")
    time.sleep(2)
