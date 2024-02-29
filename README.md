# Abstract:
In the age of data-driven decision-making, our project dives into the world of real-time data analysis, focusing on music streaming. By employing simulated data, we replicate the complexities of online music consumption, including user interactions, genre preferences, and playlist dynamics, enabling us to develop and evaluate advanced analytics solutions tailored to the music industry. Through the integration of Apache Kafka, Apache Spark, MongoDB, and cutting-edge machine learning techniques, we construct an end-to-end data processing pipeline capable of handling large-scale streaming data. This project explores data generation, storage, analysis, and visualization, providing valuable insights into listener behaviour, music consumption patterns, and the effectiveness of various promotional strategies.

By examining the scalability and adaptability of our analytical framework, we offer insights into the future of real-time data analysis in the dynamic and rapidly evolving landscape of music streaming services. Our findings not only contribute to advancing the understanding of user behavior in the digital music domain but also pave the way for innovative solutions to address challenges and opportunities.

# Project Background:
The primary goal of the Music Recommendation Project is to enhance user experience and engagement by providing personalized music recommendations tailored to individual preferences based on features like genre, tempo, songs, track_id, artist name.

# Objectives:
1. Personalized Music Discovery
2. Enhancing User Experience
3. Advanced Recommendation Algorithm
4. Real-time Data Analysis
5. Insightful Data Visualization

# Features in the dataset
1. track_id
2. artists
3. album_name
4. popularity
5. duration_ms
6. explicit danceability
7. energy key
8. loudness
9. mode
10. peechiness
11. acousticness
12. instrumentalness
13. liveness
14. valence
15. tempo
16. time_signature
17. track_genre

# Modules Used:
1. Apache Spark
2. Apache Kafka
3. MongoDB
4. Machine Learning
5. Tableau

# Note run both commands on two different instances of terminal

# start kafka zookeper
./run-kafka_zookeeper_server.sh -s start

# start kafka server
./run-kafka_server.sh -s start
