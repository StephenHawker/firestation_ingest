firestation_ingest.py - Ingest of Fire station location details
====================================

## Description

Extracts firestation location details from a specified URL, stores this data as csv file format.
The program then calculates the 3 closest firestations from a list of specified addresses 

## Written By Steve Hawker 19/07/2019 

## Requirements

 - Python 3.7 or later.
 - A Google Maps API key.
 - The following Python libraries
   - configparser
   - urllib 
   - import urllib.parse
   - import urllib.parse as urlparse
   - import sys
   - import math
   - import logging
   - import logging.config
   - import io
   - import ssl
   - pandas
   - haversine
   - BeautifulSoup
   - googlemaps
   - requests
   - simplejson

### API keys

Each Google Maps Web Service request requires an API key or client ID. API keys
are freely available with a Google Account at
https://developers.google.com/console. 
Note, your google account requires the following API's enabled to function
 - [Directions API]
 - [Distance Matrix API]
 - [Elevation API]
 - [Geocoding API]
 - [Geolocation API]
 - [Time Zone API]
 - [Roads API]
 - [Places API]

## Run	

Configure firestation_ingest.ini with the appropriate parameters
Edit Lookup_addresses.csv with the appropriate addresses to find the nearest n firestations
firestation_nearest.json is a json file of the addresses \ associated n nearest firestations
run firestation_ingest.py  (no parameters required) to scrape the website and create json output
Estimated travel time at 8am on a Monday / 11pm on a Thursday to the location from each firestation
this extra calculation by setting b_travel=True/False











