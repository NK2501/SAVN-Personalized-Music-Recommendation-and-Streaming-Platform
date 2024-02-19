from faker import Faker
from kafka import KafkaProducer
from json import dumps
import time
import random

KAFKA_TOPIC_NAME_CONS = "music-data"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    faker = Faker()
    Faker.seed(12345)  # Set seed for consistent data generation

    track_genres = [
        "acoustic", "afrobeat", "alt-rock", "alternative", "ambient", "anime", "black-metal", "bluegrass", "blues",
        "brazil", "breakbeat", "british", "cantopop", "chicago-house", "children", "chill", "classical", "club",
        "comedy", "country", "dance", "dancehall", "death-metal", "deep-house", "detroit-techno", "disco", "disney",
        "drum-and-bass", "dub", "dubstep", "edm", "electro", "electronic", "emo", "folk", "forro", "french", "funk",
        "garage", "german", "gospel", "goth", "grindcore", "groove", "grunge", "guitar", "happy", "hard-rock",
        "hardcore", "hardstyle", "heavy-metal", "hip-hop", "honky-tonk", "house", "idm", "indian", "indie-pop",
        "indie", "industrial", "iranian", "j-dance", "j-idol", "j-pop", "j-rock", "jazz", "k-pop", "kids", "latin",
        "latino", "malay", "mandopop", "metal", "metalcore", "minimal-techno", "mpb", "new-age", "opera", "pagode",
        "party", "piano", "pop-film", "pop", "power-pop", "progressive-house", "psych-rock", "punk-rock", "punk",
        "r-n-b", "reggae", "reggaeton", "rock-n-roll", "rock", "rockabilly", "romance", "sad", "salsa", "samba",
        "sertanejo", "show-tunes", "singer-songwriter", "ska", "sleep", "soul", "spanish", "study", "swedish",
        "synth-pop", "tango", "techno", "trance", "trip-hop", "turkish", "world-music"
    ]

    for user_id in range(1, 1001):  # Generate music data for 1000 users
        track_id = faker.uuid4()
        artists = faker.name()
        album_name = faker.company()
        track_name = faker.catch_phrase()
        popularity = random.randint(0, 100)
        duration_ms = random.randint(60000, 600000)  # Random duration between 1 min to 10 mins
        explicit = random.choice([True, False])
        danceability = random.uniform(0, 1)
        energy = random.uniform(0, 1)
        key = random.randint(0, 11)  # Assuming 12 keys in total
        loudness = random.uniform(-60, 0)  # Loudness range between -60 dB to 0 dB
        mode = random.choice([0, 1])  # Assuming 0 for minor and 1 for major
        speechiness = random.uniform(0, 1)
        acousticness = random.uniform(0, 1)
        instrumentalness = random.uniform(0, 1)
        liveness = random.uniform(0, 1)
        valence = random.uniform(0, 1)
        tempo = random.uniform(60, 200)  # Tempo range between 60 BPM to 200 BPM
        time_signature = random.choice([3, 4])  # Assuming 3/4 or 4/4 time signatures
        track_genre = random.choice(track_genres)

        user_data = {
            'User ID': user_id,
            'track_id': track_id,
            'artists': artists,
            'album_name': album_name,
            'track_name': track_name,
            'popularity': popularity,
            'duration_ms': duration_ms,
            'explicit': explicit,
            'danceability': danceability,
            'energy': energy,
            'key': key,
            'loudness': loudness,
            'mode': mode,
            'speechiness': speechiness,
            'acousticness': acousticness,
            'instrumentalness': instrumentalness,
            'liveness': liveness,
            'valence': valence,
            'tempo': tempo,
            'time_signature': time_signature,
            'track_genre': track_genre
        }

        print("Sending message:", user_data)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, value=user_data)
        time.sleep(1)
