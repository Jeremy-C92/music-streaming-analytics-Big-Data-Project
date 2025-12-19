#!/usr/bin/env python3
"""
Music Streaming Producer - Simulates real-time song plays
Sends events to Kafka topic 'song-plays'
"""

import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Top 50 songs catalog (weighted by popularity)
SONGS_CATALOG = [
    {"song_id": "S001", "title": "Cruel Summer", "artist": "Taylor Swift", "weight": 10},
    {"song_id": "S002", "title": "Anti-Hero", "artist": "Taylor Swift", "weight": 9},
    {"song_id": "S003", "title": "Blinding Lights", "artist": "The Weeknd", "weight": 10},
    {"song_id": "S004", "title": "Save Your Tears", "artist": "The Weeknd", "weight": 8},
    {"song_id": "S005", "title": "Un Verano Sin Ti", "artist": "Bad Bunny", "weight": 9},
    {"song_id": "S006", "title": "Titi Me Preguntó", "artist": "Bad Bunny", "weight": 7},
    {"song_id": "S007", "title": "Flowers", "artist": "Miley Cyrus", "weight": 9},
    {"song_id": "S008", "title": "Kill Bill", "artist": "SZA", "weight": 8},
    {"song_id": "S009", "title": "Creepin'", "artist": "Metro Boomin", "weight": 7},
    {"song_id": "S010", "title": "Unholy", "artist": "Sam Smith", "weight": 8},
    {"song_id": "S011", "title": "As It Was", "artist": "Harry Styles", "weight": 9},
    {"song_id": "S012", "title": "Heat Waves", "artist": "Glass Animals", "weight": 7},
    {"song_id": "S013", "title": "Vampire", "artist": "Olivia Rodrigo", "weight": 8},
    {"song_id": "S014", "title": "Get Lucky", "artist": "Daft Punk", "weight": 6},
    {"song_id": "S015", "title": "Starboy", "artist": "The Weeknd", "weight": 7},
    {"song_id": "S016", "title": "Levitating", "artist": "Dua Lipa", "weight": 8},
    {"song_id": "S017", "title": "Peaches", "artist": "Justin Bieber", "weight": 7},
    {"song_id": "S018", "title": "Stay", "artist": "The Kid LAROI", "weight": 8},
    {"song_id": "S019", "title": "Good 4 U", "artist": "Olivia Rodrigo", "weight": 7},
    {"song_id": "S020", "title": "Shivers", "artist": "Ed Sheeran", "weight": 7},
    {"song_id": "S021", "title": "Dance Monkey", "artist": "Tones and I", "weight": 6},
    {"song_id": "S022", "title": "Dynamite", "artist": "BTS", "weight": 7},
    {"song_id": "S023", "title": "Butter", "artist": "BTS", "weight": 6},
    {"song_id": "S024", "title": "Montero", "artist": "Lil Nas X", "weight": 6},
    {"song_id": "S025", "title": "Industry Baby", "artist": "Lil Nas X", "weight": 6},
    {"song_id": "S026", "title": "Bad Habit", "artist": "Steve Lacy", "weight": 7},
    {"song_id": "S027", "title": "About Damn Time", "artist": "Lizzo", "weight": 7},
    {"song_id": "S028", "title": "Running Up That Hill", "artist": "Kate Bush", "weight": 6},
    {"song_id": "S029", "title": "Watermelon Sugar", "artist": "Harry Styles", "weight": 7},
    {"song_id": "S030", "title": "Mood", "artist": "24kGoldn", "weight": 6},
    {"song_id": "S031", "title": "positions", "artist": "Ariana Grande", "weight": 7},
    {"song_id": "S032", "title": "34+35", "artist": "Ariana Grande", "weight": 6},
    {"song_id": "S033", "title": "Circles", "artist": "Post Malone", "weight": 7},
    {"song_id": "S034", "title": "Rockstar", "artist": "Post Malone", "weight": 7},
    {"song_id": "S035", "title": "Sunflower", "artist": "Post Malone", "weight": 7},
    {"song_id": "S036", "title": "Señorita", "artist": "Shawn Mendes", "weight": 6},
    {"song_id": "S037", "title": "Memories", "artist": "Maroon 5", "weight": 6},
    {"song_id": "S038", "title": "thank u, next", "artist": "Ariana Grande", "weight": 7},
    {"song_id": "S039", "title": "Shallow", "artist": "Lady Gaga", "weight": 6},
    {"song_id": "S040", "title": "Someone You Loved", "artist": "Lewis Capaldi", "weight": 6},
    {"song_id": "S041", "title": "Happier", "artist": "Marshmello", "weight": 6},
    {"song_id": "S042", "title": "Without Me", "artist": "Halsey", "weight": 6},
    {"song_id": "S043", "title": "Sucker", "artist": "Jonas Brothers", "weight": 5},
    {"song_id": "S044", "title": "7 rings", "artist": "Ariana Grande", "weight": 7},
    {"song_id": "S045", "title": "Old Town Road", "artist": "Lil Nas X", "weight": 8},
    {"song_id": "S046", "title": "Bad Guy", "artist": "Billie Eilish", "weight": 8},
    {"song_id": "S047", "title": "Happier Than Ever", "artist": "Billie Eilish", "weight": 7},
    {"song_id": "S048", "title": "Ocean Eyes", "artist": "Billie Eilish", "weight": 6},
    {"song_id": "S049", "title": "Lovely", "artist": "Billie Eilish", "weight": 6},
    {"song_id": "S050", "title": "Everything I Wanted", "artist": "Billie Eilish", "weight": 6},
]


class MusicStreamProducer:
    def __init__(self, bootstrap_servers='localhost:29092', topic='song-plays'):
        """Initialize Kafka producer with retries"""
        self.topic = topic
        self.producer = None
        self.bootstrap_servers = bootstrap_servers
        self._connect()
        
    def _connect(self):
        """Connect to Kafka with retry logic"""
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',  # Wait for all replicas
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                    #compression_type='snappy',
                    batch_size=32768,
                    linger_ms=10
                )
                logger.info(f"✓ Connected to Kafka at {self.bootstrap_servers}")
                return
            except Exception as e:
                retry_count += 1
                logger.warning(f"Connection attempt {retry_count}/{max_retries} failed: {e}")
                time.sleep(5)
        
        raise Exception("Could not connect to Kafka after maximum retries")
    
    def generate_event(self):
        """Generate a realistic song play event with weighted selection"""
        weights = [song['weight'] for song in SONGS_CATALOG]
        song = random.choices(SONGS_CATALOG, weights=weights, k=1)[0]
        
        event = {
            'song_id': song['song_id'],
            'title': song['title'],
            'artist': song['artist'],
            'user_id': f"U{random.randint(1000, 9999)}",
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'duration_ms': random.randint(30000, 300000),  # 30s to 5min
            'completed': random.random() > 0.3,  # 70% completion rate
            'device': random.choice(['mobile', 'desktop', 'smart_speaker', 'tablet']),
            'country': random.choice(['US', 'UK', 'CA', 'AU', 'FR', 'DE', 'ES', 'BR', 'MX', 'JP'])
        }
        return event
    
    def send_event(self, event):
        """Send event to Kafka with error handling"""
        try:
            future = self.producer.send(
                self.topic,
                key=event['song_id'],
                value=event
            )
            record_metadata = future.get(timeout=10)
            return True
        except KafkaError as e:
            logger.error(f"Failed to send event: {e}")
            return False
    
    def run(self, events_per_second=10, duration_seconds=None):
        """
        Main loop to generate and send events
        
        Args:
            events_per_second: Target rate (default: 10)
            duration_seconds: Run for N seconds (None = infinite)
        """
        logger.info(f"🎵 Starting producer - {events_per_second} events/sec")
        logger.info(f"📊 Publishing to topic: {self.topic}")
        
        interval = 1.0 / events_per_second
        events_sent = 0
        start_time = time.time()
        
        try:
            while True:
                loop_start = time.time()
                event = self.generate_event()
                if self.send_event(event):
                    events_sent += 1
                    if events_sent % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = events_sent / elapsed
                        logger.info(f"📈 Sent {events_sent} events | Rate: {rate:.1f}/sec")
                # Check duration limit
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    logger.info(f"✓ Reached duration limit: {duration_seconds}s")
                    break
                
                # Sleep to maintain rate
                elapsed = time.time() - loop_start
                sleep_time = max(0, interval - elapsed)
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Shutting down producer...")
        finally:
            self.close()
            
    def close(self):
        """Flush and close producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("✓ Producer closed gracefully")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Music Streaming Event Producer')
    parser.add_argument('--rate', type=int, default=10, help='Events per second')
    parser.add_argument('--duration', type=int, help='Duration in seconds (optional)')
    parser.add_argument('--broker', default='localhost:29092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='song-plays', help='Kafka topic name')
    
    args = parser.parse_args()
    
    producer = MusicStreamProducer(
        bootstrap_servers=args.broker,
        topic=args.topic
    )
    producer.run(
        events_per_second=args.rate,
        duration_seconds=args.duration
    )