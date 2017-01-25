from pymongo import MongoClient
import requests
from flask import Flask, jsonify, request
from urllib2 import urlopen
import json


app = Flask(__name__)

client = MongoClient('127.0.0.1:27017')
climate_analysis = client['climate_analysis']
climate_data_geolocation = climate_analysis['climate_data_geolocation']


def getplace(lat, lon):
    url = "http://maps.googleapis.com/maps/api/geocode/json?"
    url += "latlng=%s,%s&sensor=false" % (lat, lon)
    v = urlopen(url).read()
    j = json.loads(v)
    components = j['results'][0]['address_components']
    country = town = None
    for c in components:
        if "country" in c['types']:
            country = c['long_name']
        if "postal_town" in c['types']:
            town = c['long_name']
    return town, country

@ app.route("/get_average_seasonal_data", methods = ['POST'])
def get_average_seasonal_data():

    latitude = request.form.get('latitude')
    longitude = request.form.get('longitude')
    

    data = climate_data_geolocation.find({
    'LAT_X': {
        '$gte': float(latitude) - 1,
        '$lte': float(latitude)
    },
    'LAT_Y': {
        '$gte': float(latitude),
        '$lte': float(latitude) + 1
    },
    'LON_X': {
        '$gte': float(longitude) - 1,
        '$lte': float(longitude)
    },
    'LON_Y': {
        '$gte': float(longitude),
        '$lte': float(longitude) + 1
    }
})
    
    response_dict = {}
    response_dict['coordinates'] = str((latitude,longitude))
    response_dict['average_weather_by_season_year'] = []
    response_dict['weather_stations'] = []

    if data.count() > 0:
    	response_dict['country'] = getplace(float(latitude),float(longitude))[1]
        for items in data:
            single_dict = {}
            single_dict['Average_Temperature'] = items['TAVG']
            single_dict['Season'] = items['SEASON']
            single_dict['Year'] = items['YEAR']
            single_dict['Station_Name'] = items['STATION']
            response_dict['average_weather_by_season_year'].append(single_dict)
            response_dict['weather_stations'].append(items['_id'])
    else:
        response_dict['message'] = "No data found for coordinates" + str((latitude,longitude))

    return jsonify(response_dict)




if __name__ == '__main__':
    app.run(debug = True, host = '0.0.0.0', port = 8000)