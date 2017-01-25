
# coding: utf-8

# In[42]:

from pymongo import MongoClient
from os import listdir
import math
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContext = SQLContext(sc)

#Establish connection with local Mongo Client
client = MongoClient('127.0.0.1:27017')
#Create new database
climate_analysis=client['demo_climate_analysis']

#Create collection for ingesting raw data
climate_analysis_temp_data = climate_analysis['temp_data_by_station']

csv_files = listdir('gson_demo') #Get list of all csv files in extracted gsom folder

def filter_and_export_to_mongo(f):
    #Using SparkSQL to read csv data and get dataframe for each csv
    df = sqlContext.read.format("com.databricks.spark.csv").option("inferschema", "true").option("header", "true")\
    .option("mode", "DROPMALFORMED").load("gson_demo/"+f)


    post_1900_df = df.where(df.DATE >='1900-01')
    
    #Extract relevant fields for seasonal temperature only i.e. STATION_NAME,DATE and TAVG
    gd = post_1900_df.rdd.map(lambda x: (x.STATION+'_'+x.DATE,x.STATION,x.DATE,x.TAVG) if 'TAVG' in x.__fields__ and x.TAVG is not None  else  (x.STATION+'_'+x.DATE,x.STATION,x.DATE,'NA'))
    if not gd.isEmpty():
        pd = gd.toDF().toPandas()
        #_id column will serve as an index column in mongo to speed up lookups.
        pd.columns = ['_id','STATION','DATE','TAVG']
        #Perform bulk insertion using MongoDB insert_many method
        climate_analysis_temp_data.insert_many(pd.to_dict('records'))

    
def extract_location_data(line):
    loc_data = line.split(' ')
    loc_data = [x for x in loc_data if x.strip()!='']
    return [loc_data[0],loc_data[1],loc_data[2]]

def get_station_geolocation_df(ghcnd_stations):
    stations_geo_rdd = sc.textFile(ghcnd_stations)
    stations_geo_df = stations_geo_rdd.map(extract_location_data).toDF().toPandas()
    stations_geo_df.columns = ['STATION','LATITUDE','LONGITUDE']
    #Create Global Geographic Grid for 1x1 granularity
    stations_geo_df['lat_range'] = stations_geo_df['LATITUDE'].map(lambda x: (math.floor(float(x)),math.ceil(float(x))))
    stations_geo_df['lon_range'] = stations_geo_df['LONGITUDE'].map(lambda x: (math.floor(float(x)),math.ceil(float(x))))
    return stations_geo_df


def get_season(date):
    month = int(date.split('-')[1])
    
    if month>=3 and month<=5:
        season = 'SPRING'
    elif month>=6 and month<=8:
        season = 'SUMMER'
    elif month>=9 and month <=11:
        season = 'FALL'
    else:
        season = 'WINTER'
        
    return season

def get_location_group(station,df):
    #Generating corrdinate pairs for lower left and upper right corners of a 1x1 square
    station_rec = df[df['STATION'] == station ]
    return station_rec['lat_range'].iloc[0][0],station_rec['lon_range'].iloc[0][0], \
    station_rec['lat_range'].iloc[0][1],station_rec['lon_range'].iloc[0][1] 
    
    
def ingest_seasonal_yearly_station_data(stations_geo_df):
    climate_data_geolocation = climate_analysis['climate_data_geolocation']
    for stations in [x.replace('.csv','') for x in csv_files]:
#         print stations
        station_objects = []
        lat_x,lon_x,lat_y,lon_y = get_location_group(stations,stations_geo_df)

        #Finding list of sations for Average Temp data excluding NA values

        rec = climate_analysis_temp_data.find({'_id':{'$gte':stations+'_1900-01','$lte':stations+'_2017-01'},'TAVG':{'$ne':'NA'}})
        if rec.count() > 0:
            for recs in rec:
                if not math.isnan(float(recs['TAVG'])):
                    new_station_dict = {}
                    new_station_dict['SEASON'] = get_season(recs['DATE'])
                    new_station_dict['TAVG'] = float(recs['TAVG']) 
                    new_station_dict['_id'] = recs['_id']
                    new_station_dict['YEAR'] = recs['_id'].split('_')[1][:4]
                    station_objects.append(new_station_dict)

            station_store_pd = pd.DataFrame(station_objects)
            if station_store_pd.columns != []:    

                #Aggregating temprature by season
                station_store_pd_grouped = station_store_pd.groupby(['SEASON','YEAR']).mean().reset_index()

                station_store_pd_grouped['LAT_X'] = lat_x
                station_store_pd_grouped['LON_X'] = lon_x
                station_store_pd_grouped['LAT_Y'] = lat_y
                station_store_pd_grouped['LON_Y'] = lon_y
                station_store_pd_grouped['STATION'] = stations
                station_store_pd_grouped['_id'] = stations+"_"+station_store_pd_grouped['SEASON']+'-'+station_store_pd_grouped['YEAR']

                #Bulk insertion to new table which will serve as database for our endpoint
                
                climate_data_geolocation.insert_many(station_store_pd_grouped.to_dict('records'))
                
                


# # Creating a normalized geolocated seasonal temperature average Mongo collection 'climate_data_geolocation'

# In[43]:

if __name__ == '__main__':
    for csv in csv_files:
        filter_and_export_to_mongo(csv)
    station_geolocation_df = get_station_geolocation_df('ghcnd-stations.txt')
    ingest_seasonal_yearly_station_data(station_geolocation_df)
    
    
        

