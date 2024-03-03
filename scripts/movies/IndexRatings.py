import csv
from collections import deque
import elasticsearch
from elasticsearch import helpers

def readMovies():
    csvfile = open('../../data/ml-latest-small/movies.csv', 'r', encoding="utf8")

    reader = csv.DictReader(csvfile)

    titleLookup = {}

    for movie in reader:
        titleLookup[movie['movieId']] = movie['title']

    return titleLookup

def readRatings():
    csvfile = open('../../data/ml-latest-small/ratings.csv', 'r', encoding="utf8")

    titleLookup = readMovies()

    reader = csv.DictReader(csvfile)
    for line in reader:
        rating = {}
        rating['user_id'] = int(line['userId'])
        rating['movie_id'] = int(line['movieId'])
        rating['title'] = titleLookup[line['movieId']]
        rating['rating'] = float(line['rating'])
        rating['timestamp'] = int(line['timestamp'])
        yield rating


es = elasticsearch.Elasticsearch(["http://127.0.0.1:9200"])

# Delete the index if it exists
es.indices.delete(index="ratings", ignore=[400, 404])

# Bulk indexing the ratings
deque(helpers.parallel_bulk(es, readRatings(), index="ratings"), maxlen=0)

# Refresh the index to make changes visible
es.indices.refresh()
