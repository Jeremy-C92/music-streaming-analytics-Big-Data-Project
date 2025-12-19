import time
import json
import random
from kafka import KafkaProducer
from faker import Faker

# Configuration
TOPIC = 'music_streams'
BOOTSTRAP_SERVER = 'localhost:9092'

print(f"Initialisation du Producer vers {BOOTSTRAP_SERVER}...")

# Attente active que Kafka soit prêt (utile au démarrage)
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connecté à Kafka !")
    except Exception as e:
        print("Kafka pas encore prêt, nouvelle tentative dans 5s...")
        time.sleep(5)

GENRES = ['Rock', 'Pop', 'Jazz', 'Rap', 'Techno']
fake = Faker()

try:
    print("Début de l'envoi des streams...")
    while True:
        data = {
            'user_id': random.randint(1, 1000),
            'song_id': random.randint(1, 100),
            'genre': random.choice(GENRES),
            'timestamp': time.time(),
            'artist': fake.name(),
            'track': fake.sentence(nb_words=3)
        }
        producer.send(TOPIC, data)
        print(f"[LIVE] Sent: {data['genre']} - {data['track']}")
        time.sleep(1) # 1 message par seconde
except KeyboardInterrupt:
    print("Arrêt du producer.")
    producer.close()