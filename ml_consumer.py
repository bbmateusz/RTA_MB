from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json, requests

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='ml-scoring',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

API_URL = "http://localhost:8001/score"

print("ML consumer wystartował. Czekam na transakcje...")

for message in consumer:
    tx = message.value

    # 1. Wyciągnij cechy, których oczekuje API
    features = {
        "amount": tx["amount"],
        "is_electronics": tx.get("is_electronics", 0),
        "tx_per_minute": tx.get("tx_per_minute", 5)
    }

    # 2. Zapytaj API o ocenę
    response = requests.post(API_URL, json=features)
    result = response.json()

    # 3. Jeśli fraud — alert na temat 'alerts' + wypisz
    if result["is_fraud"]:
        alert = {
            "transaction": tx,
            "fraud_probability": result["fraud_probability"],
            "detected_at": datetime.now().isoformat()
        }
        alert_producer.send('alerts', alert)
        alert_producer.flush()
        print(f"🚨 ALERT! fraud_prob={result['fraud_probability']:.2f} | {tx}")
    else:
        print(f"   ok    fraud_prob={result['fraud_probability']:.2f} | amount={tx['amount']}")
