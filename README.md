# Spark-Streaming-Hate-Tweet-Detection
Real Time Big Data Analytics with Spark Streaming and Spark MLlib

## Project Abstract
With the rise of micro-blogging website Twitter, people now have the freedom to freely express their thoughts and feelings in the online realm, which often tend to be highly opinionated. Officially, Twitter has reported 500 million tweets per day, way back in 2013. This number has only grown ever since and is expected to continue the upward trend. This makes it an interesting and challenging avenue for distributed systems to process information at such massive scale and be able to identify those highly opinionated, hateful and/or abusive tweets, to promote a safer online community.

With this project, we implement real time detection of hateful or abusive tweets with Apache Spark. The primary objective of this project to leverage the extended capabilities of Apache Spark with streaming, machine learning libraries and pipelines. We use Spark machine learning library “MLlib” to train our model on a 100,000-tweet dataset. We use term frequency-inverse document frequency (td-idf) features from the tweets in the dataset and train a logistic regression model with a pipeline to classify tweets. We then use Spark Streaming to connect with Twitter and retrieve tweets to identify the hateful or abusive tweets in real-time.

Our model has achieved an accuracy rate of about 93% on the testing dataset.

## Instructions to run the code
Copy the contents of the repository to a folder on cluster.

1. Build the application with below command.
```bash
mvn clean package
```

2. Execute BuildPipeline class to train our model and build a pipeline
```bash
spark-submit --class ca.uwaterloo.cs451.project.BuildPipeline \
   target/assignments-1.0.jar --input-path data --model-path Model/lr-model
```

3. Execute StreamTwitter class to stream live tweets from Twitter and identify hateful/abusive tweets
```bash
mvn clean package
```

## Acknowledgement
I would like to thank Antigoni-Maria for providing this amazing crowdsourced Twitter dataset with appropriate labels.

Link - https://github.com/ENCASEH2020/hatespeech-twitter


