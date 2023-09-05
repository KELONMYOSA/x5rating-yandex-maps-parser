import os
from datetime import datetime

import clickhouse_connect
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST')
CLICKHOUSE_USERNAME = os.environ.get('CLICKHOUSE_USERNAME')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD')

client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, username=CLICKHOUSE_USERNAME, password=CLICKHOUSE_PASSWORD)


def insert_place_data(place_data):
    metro = []
    for metro_data in place_data['metro']:
        metro.append({
            'id': metro_data['id'],
            'name': metro_data['name'],
            'distanceValue': str(metro_data['distanceValue']),
        })

    stops = []
    for stops_data in place_data['stops']:
        stops.append({
            'id': stops_data['id'],
            'name': stops_data['name'],
            'distanceValue': str(stops_data['distanceValue']),
        })

    chain = {}
    if 'chain' in list(place_data.keys()):
        chain_quantity = ""
        if 'quantityInCity' in list(place_data['chain'].keys()):
            chain_quantity = str(place_data['chain']['quantityInCity'])
        chain = {
            'id': place_data['chain']['id'],
            'name': place_data['chain']['name'],
            'quantityInCity': chain_quantity,
        }

    features = []
    if 'features' in list(place_data.keys()):
        for features_data in place_data['features']:
            feature_name = ""
            if 'name' in list(features_data.keys()):
                feature_name = features_data['name']
            features.append({
                'id': features_data['id'],
                'name': feature_name,
                'value': str(features_data['value']),
            })

    aspects = []
    if 'aspects' in list(place_data.keys()):
        aspects = []
        for aspects_data in place_data['aspects']:
            aspects_name = ""
            if 'name' in list(aspects_data.keys()):
                aspects_name = aspects_data['name']
            aspects.append({
                'id': aspects_data['id'],
                'name': aspects_name,
                'count': str(aspects_data['count']),
                'positive': str(aspects_data['positive']),
                'neutral': str(aspects_data['neutral']),
                'negative': str(aspects_data['negative']),
            })

    data = [place_data['id'],
            place_data['title'],
            place_data['coordinates'],
            place_data['compositeAddress'],
            place_data['ratingData'],
            place_data['workingTimeText'],
            metro, stops, chain, features, aspects]

    column_names = ['id', 'title', 'coordinates', 'address', 'rating', 'working_time',
                    'metro', 'stops', 'chain', 'features', 'aspects']

    client.insert('ymaps_shops', [data], column_names)


def insert_review_data(review_data):
    reviews = review_data["reviews"]
    data = []

    for r in reviews:
        public_id = ""
        profession_level = ""
        if 'author' in list(r.keys()):
            public_id = r['author']['publicId']
            profession_level = r['author']['professionLevel']

        try:
            updated_time = datetime.strptime(r['updatedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
        except:
            updated_time = datetime.strptime(r['updatedTime'], '%Y-%m-%dT%H:%M:%SZ')

        r_data = [r['reviewId'],
                  int(r['businessId']),
                  public_id,
                  profession_level,
                  updated_time,
                  int(r['rating']),
                  r['reactions']['likes'],
                  r['reactions']['dislikes'],
                  r['textLanguage'],
                  r['text']]
        data.append(r_data)

    column_names = ['review_id', 'business_id', 'user_id', 'user_level', 'time',
                    'rating', 'likes', 'dislikes', 'text_language', 'text']

    client.insert('ymaps_reviews', data, column_names)
