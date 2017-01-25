# Climate-Analysis
##Global monthly summary data: https://www.ncei.noaa.gov/data/gsom/archive/gsom_latest.tar.gz 
##Documentation: https://www1.ncdc.noaa.gov/pub/data/cdo/documentation/gsom-gsoy_documentation.pdf 
##Station Data (also includes geolocation data):https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt 
##Unzcompressed dataset is around 5.8GB

##Requirements:
    1.MongoDB
    2.Pandas
    3.PySpark
    4.PyMongo(Mongo Client for Python)


##Instructions to run:
1. In order to run the analysis, the data first needs to be ingested.
2. In order to run the Ingestion Script via spark-submit, the location of the folder containing the gsom csv files and the ghcnd-station.txt have to be specified in the 'Data_Cleaning_and_Ingestion.py' on lines (24,29) and 127 respectively. Please make sure the MongoClient connection string is according to your own system.
4. Once the ingestion script is run 100% via spark-submit, you should start the `gsom_endpoint.py` which will start the flask wsgi server.
5. Once server is started you can send a POST request with (lat,lon) params e.g `curl --data "latitude=43&longitude=-79" localhost:8000/get_average_seasonal_data`
6. For user convenience a snapshot of the `climate_data_geolocation` collection in `bson` format can be downloaded from the link `https://drive.google.com/file/d/0B4Hi9FUQvNzqbDRCRnpjTEtneWs/view?usp=sharing` . The user can simply import this collection by extracting the .bson file and then importing via `mongorestore -d <database_name> <extracted_directory_path>` on the command line terminal. This is done so that the user does not have to run the 'Data_Cleaning_and_Ingestion.py' which takes more than an hour to complete on the entire dataset.
7. A successful query will return the following fields:
  1. List of json objects of `average_weather_by_season_year`
  2. List of available `weather_stations` by Year and season for the given coordinates 
  3. Country of the weather station/s
  4. Queried cooridinates (lat,lon)
                  
 ##Sample response for coordinates (35,69):
     `{
        "average_weather_by_season_year": [
            {
                "Average_Temperature": 6.96,
                "Season": "FALL",
                "Station_Name": "AFM00040948",
                "Year": "1979"
            },
            {
                "Average_Temperature": 19.54,
                "Season": "FALL",
                "Station_Name": "AFM00040948",
                "Year": "1980"
            }
        ],
        "coordinates": "(u'35', u'69')",
        "country": "Afghanistan",
        "weather_stations": [
            "AFM00040948_FALL-1979",
            "AFM00040948_FALL-1980"
        ]
    }`
 
 
 
