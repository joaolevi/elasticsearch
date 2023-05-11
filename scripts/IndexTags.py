import csv
from collections import deque
import elasticsearch
from elasticsearch import helpers

def readMovies():
    csvfile = open('data/ml-latest-small/movies.csv', 'r', encoding="utf8")

    reader = csv.DictReader( csvfile )

    titleLookup = {}

    for movie in reader:
            titleLookup[movie['movieId']] = movie['title']

    return titleLookup

def readRatings():
    csvfile = open('data/ml-latest-small/tags.csv', 'r', encoding="utf8")

    titleLookup = readMovies()

    reader = csv.DictReader( csvfile )
    for line in reader:
        tags = {}
        tags['user_id'] = int(line['userId'])
        tags['movie_id'] = int(line['movieId'])
        tags['title'] = titleLookup[line['movieId']]
        tags['tag'] = str(line['tag'])
        tags['timestamp'] = int(line['timestamp'])
        yield tags


es = elasticsearch.Elasticsearch(["http://127.0.0.1:9200"])

es.indices.delete(index="tags",ignore=404)
deque(helpers.parallel_bulk(es,readRatings(),index="tags"), maxlen=0)
es.indices.refresh()
