import pandas as pd
import numpy as np
from pymongo import MongoClient


class SpotifyRecommender():
    def __init__(self, train_data):
        # Initialize with the training dataset
        self.train_data = train_data

    def train_model(self):
        # Train the model using the provided training data
        # Add your training logic here
        pass

    def get_recommendations(self, input_features, amount=20):
        # Calculate distances for each song in the training dataset based on input features
        distances = []
        for _, song in self.train_data.iterrows():
            dist = 0
            for col in self.train_data.columns:
                if col in input_features:
                    # Calculate Manhattan distance for each selected feature
                    dist += np.absolute(float(input_features[col]) - float(song[col]))
            distances.append(dist)

        # Add distances to the training dataset
        self.train_data['distance'] = distances

        # Sort the dataset by distance and select top recommendations
        top_recommendations = self.train_data.sort_values('distance').head(amount)

        # Extract song names, artists, and albums from recommendations
        recommendations = top_recommendations[['track_name', 'artists', 'album_name']]

        return recommendations


def get_user_inputs_from_mongodb():
    # Connect to MongoDB and retrieve input features for each user
    client = MongoClient('mongodb://localhost:27017/')
    db = client['project_4']
    collection = db['music_data']

    # Assuming each document in the collection represents a user with their input features
    user_inputs = {}
    for user_doc in collection.find():
        user_id = user_doc['User ID']
        user_features = {
            'danceability': user_doc['danceability'],
            'energy': user_doc['energy'],
            'key': user_doc['key'],
            'loudness': user_doc['loudness'],
            'mode': user_doc['mode'],
            'speechiness': user_doc['speechiness'],
            'acousticness': user_doc['acousticness'],
            'instrumentalness': user_doc['instrumentalness'],
            'liveness': user_doc['liveness'],
            'valence': user_doc['valence'],
            'tempo': user_doc['tempo'],
            'duration_ms': user_doc['duration_ms'],
            'time_signature': user_doc['time_signature']
        }
        user_inputs[user_id] = user_features

    return user_inputs


def main():
    # Initialize the recommender with an empty DataFrame (we'll train the model later)
    recommender = SpotifyRecommender(pd.DataFrame())

    # Get input features for each user from MongoDB
    user_inputs = get_user_inputs_from_mongodb()

    # Train the model (if needed)
    # Load training data from CSV file
    train_data = pd.read_csv('/home/talentum/clean_data.csv')  # Replace with your CSV filename
    recommender.train_data = train_data

    # Recommend songs for each user
    for user_id, input_features in user_inputs.items():
        print(f"User ID: {user_id}")
        recommendations = recommender.get_recommendations(input_features)
        print("Recommended songs, artists, albums:")
        print(recommendations)
        print()


if __name__ == "__main__":
    main()