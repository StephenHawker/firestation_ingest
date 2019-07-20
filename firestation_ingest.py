"""Read Firestations web page data """
import datetime
import time
import argparse
import configparser
import urllib
import urllib.request
import urllib.parse
import urllib.parse as urlparse
import sys
import math
import logging
import logging.config
import io
import csv
import simplejson as json
import ssl
import pandas
import haversine
from haversine import haversine
import pyodbc as pyodbc
from bs4 import BeautifulSoup
from collections import defaultdict
import googlemaps
import requests
import codecs
import re


class DatabaseLoadError(Exception):
    """Database Exception

    Keyword arguments:
    Exception -- Exception
    """
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return repr(self.data)


class FirestationIngestError(Exception):
    """FirestationIngestError Exception

    Keyword arguments:
    Exception -- Exception
    """
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return repr(self.data)


############################################################
# Load Config File
############################################################
def load_config(file):
    """Load config file

    Keyword arguments:
    file -- config file path
    config -- config array
    """
    config = {}

    config = config.copy()
    cp = configparser.ConfigParser()
    cp.read(file)
    for sec in cp.sections():
        name = sec.lower()  # string.lower(sec)
        for opt in cp.options(sec):
            config[name + "." + opt.lower()] = cp.get(sec, opt).strip()
    return config


############################################################
# Main
############################################################
def main():
    """Main process

    """
    try:

        LOGGER.info('Started run. main:')

        firestation_url = configImport["firestation.url"]
        temp_file = configImport["firestation.temp_file"]
        csv_file = configImport["firestation.csv_file"]
        number_closest = configImport["firestation.number_closest"]
        postcode_api_url = configImport["firestation.postcode_api_url"]
        distance_matrix_api_url = configImport["firestation.distance_matrix_url"]

        lkp_addresses_file = configImport["firestation.lookup_addresses_file"]
        table_class = configImport["firestation.table_class"]
        f_value = configImport["firestation.form_value"]
        firestation_nearest_json_File = configImport["firestation.json_file"]
        firestation_nearest_json_with_travel = configImport["firestation.json_file_with_travel"]
        b_travel = bool(["firestation.b_travel"])

        LOGGER.debug("firestation URL : %s ", firestation_url)
        LOGGER.debug("temp_file : %s ", temp_file)
        LOGGER.debug("csv_file : %s ", csv_file)
        LOGGER.debug("Postcode api URL : %s ", postcode_api_url)
        LOGGER.debug("lkp_addresses_file : %s ", lkp_addresses_file)
        LOGGER.debug("number_closest : %s ", str(number_closest))

        firestatiom_html = get_pagedata(firestation_url, f_value)

        #TODO Deal with changes and updates

        with io.open(temp_file, "w", encoding="utf-8") as f:
            f.write(firestatiom_html)

        the_table = get_table(firestatiom_html, table_class)

        save_tab_as_csv(the_table, csv_file)

        #read back firestation data in to do calcs
        df_fs = pandas.read_csv(csv_file)

        #get lat/lon for the file of lookup addresses
        df_lkp = get_lat_long_lkp_addresses(lkp_addresses_file)

        #Process lookups to get top n, get as json fragment
        process_lkp_list(df_lkp, df_fs, top_n=number_closest, json_file=firestation_nearest_json_File, b_travel=b_travel)

        #Process lookups to get top n, and get travel time as json fragment
        #process_lkp_list_with_travel(df_lkp, df_fs, top_n=number_closest, json_file=firestation_nearest_json_with_travel)

        LOGGER.info('Completed run.')


    except FirestationIngestError as recex:
        LOGGER.error("An Exception occurred Firestation Ingest  ")
        LOGGER.error(recex.data)
        raise FirestationIngestError(recex)

    except Exception:
        LOGGER.error("An Exception occurred Firestation Ingest ")
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("!")


############################################################
# Get page html
############################################################
def get_pagedata(site_url, form_value):
    """get html from page

    Keyword arguments:
    site_url -- Site URL to post to
    form_values -- form values to post
    """
    form_val = {form_value : '%', 'Submit' : 'Select'}
    form_data = urllib.parse.urlencode(form_val)
    header = {"Content-type": "application/x-www-form-urlencoded",
              "Accept": "text/plain", "Referer": "http://www.firestations.org.uk/Fire_Stations_Page.php"}

    header2 = {}

    header2[
        'User-Agent'] = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36"
    header2['Accept'] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    header2['Content-type'] = "application/x-www-form-urlencoded"

    # body = form_data.encode(encoding='utf-8')

    s = requests.Session()

    # providers = s.post(url, params=form_data, data=form_data, timeout=15, verify=True,headers=header)

    # print (providers.content)

    # Open a sesssion first
    s.get(site_url)
    # Post form data to get
    r = s.post(site_url, data=form_data, headers=header2, verify=True)

    output_html = str(r.text)

    LOGGER.debug("Status: %s", str(r.status_code))
    LOGGER.debug("reason: %s", str(r.reason))

    return output_html

############################################################
# Get table of data
############################################################
def get_table(html, table_class):
    """get table from html page

    Keyword arguments:
    html -- html
    table_class -- class of table to get
    """
    soup = BeautifulSoup(html, features="html.parser")
    table = soup.find('table', {'class': table_class})

    return table

############################################################
# take table of data and convert to csv
############################################################
def save_tab_as_csv(tab, csv_file):
    """get table from html page

    Keyword arguments:
    tab -- table
    csv_file - csv file to write to
    """

    output_rows = []
    row_marker = 0
    td_count = 0

    for table_row in tab.findAll('tr'):
        #row 1, store column count and append extra lat & lon cols
        if row_marker == 1:
            td_count = column_marker
            output_row.append("lat")
            output_row.append("lon")

        column_marker = 0

        row_marker += 1

        columns = table_row.findAll('td')
        output_row = []

        for column in columns:

            column_marker += 1

            #Get link in detail for first column in row
            if column_marker == 1:
                fs_link = ""

                for link in column.findAll('a', href=True):
                    #print(link['href'])
                    fs_link = link['href']
                    #Get latitude qs value
                    lat = get_qs_value(fs_link, 'lat')
                    #print("lat: " + str(lat))
                    #Get longitude qs value
                    lon = get_qs_value(fs_link, 'lon')
                    #print("lon: " + str(lon))
                    #TODO get_accuracy(row)

                # Append first column header as not in data
                if row_marker == 1:
                    output_row.append("Detail")
                else:
                    output_row.append(column.text + " " + fs_link)

            else:
                output_row.append(column.text)

             #append extra derived cols
            if column_marker == td_count:
                output_row.append(str(lat))
                output_row.append(str(lon))

        output_rows.append(output_row)

    LOGGER.debug("cols: %s", str(td_count))

    ##Save to CSV
    with open(csv_file, 'w', encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(output_rows)

############################################################
# TODO - Get accuracy based on font colour
############################################################
def get_accuracy(row_string):
    """get table from html page

    Keyword arguments:
    row_string -- html row string
    """
   # WHITE = plot is accurate(i.e.on correct building) - style='color:white;'
   # RED = plot is not accurate(i.e. not on correct building) - style='color:red;'
   # YELLOW = plot is very rough(i.e.only at start of street)- style='color:yellow;'
   # BLUE = no plot - can you help?- style='color:blue;'

############################################################
# Process the base addresses, get lat/lon from postcode
############################################################
def get_lat_long_lkp_addresses(lkp_addresses_file):
    """Get firestation lookup addresses from file

    Keyword arguments:
    lkp_addresses_file -- file of lookup addresses
    df_fs -- dataframe of firestations
    """

    postcode_api_url = configImport["firestation.postcode_api_url"]

    # Cater for numeric string fields
    df = pandas.read_csv(lkp_addresses_file,
                         dtype={'address1': 'S100', 'address2': 'S100'})

    lat_list = []
    lon_list = []

    for index, row in df.iterrows():

        pc_json = do_postcode_lookup(postcode_api_url, row['postcode'])

        json_str = json.dumps(pc_json)
        resp = json.loads(json_str)

        lat_value = resp['result']['latitude']
        lon_value = resp['result']['longitude']

        lon_list.append(resp['result']['longitude'])
        lat_list.append(resp['result']['latitude'])

    df['longitude'] = lon_list
    df['latitude'] = lat_list

    return df


############################################################
# Process the list of lookups and get the list of closest n
# firestations for each
############################################################
def process_lkp_list(df_lkp, firestations_df, top_n, json_file, b_travel):
    """Process the list of lookup addresses to get nearest n

    Keyword arguments:
    lkp_list -- base point (lat_value, lon_value)
    firestations_df -- data frame of firestation data
    """
    r_list = defaultdict(list)
    lkp_list = []

    for index, row in df_lkp.iterrows():

        nearest_list = []

        lat_value = row['latitude']
        lon_value = row['longitude']

        LOGGER.debug("process_lkp_list :lat_value: %s", str(lat_value))
        LOGGER.debug("process_lkp_list :lon_value: %s", str(lon_value))

        lkp_list = df_lkp.values[index].tolist()

        base_point = (lat_value, lon_value)  # (lat, lon)

        #Return sorted list of nearest by distance
        df_fs_dist_list = create_nearest_list(base_point, firestations_df=firestations_df, top_n=top_n, bln_travel_times=b_travel)

        lkp_list.append(df_fs_dist_list)

        r_list["lkpaddress"].append(lkp_list)
        #r_list["neareststations"].append(df_fs_dist_list)

    #save as json
    json.dump(r_list, open(json_file, "w"))

############################################################
# Read firestations into a data frame, take a vector point passed
# and return an ordered list from nearest ASC distance
############################################################
def create_nearest_list(base_point, firestations_df, top_n, bln_travel_times):
    """Create ordered list of

    Keyword arguments:
    base_point -- base point (lat_value, lon_value)
    firestations_df -- data frame of firestation data
    """

    distance_list = []

    for index, row in firestations_df.iterrows():
        lat = row['lat']
        lon = row['lon']
        point = (lat, lon)
        #Get as the crow files haversine distance
        dist = get_haversine_dist(base_point, point)

        distance_list.append(dist)

    df_fs_dist = firestations_df
    df_fs_dist['distance'] = distance_list

    #sort by distance ascending
    df_fs_dist.sort_values(by=['distance'], inplace=True)

    #Get top n
    df_fs_dist_ret = df_fs_dist[:int(top_n)]

    lst_json = []
    lc = 0

    for ix, rw in df_fs_dist_ret.iterrows():

        lst_travel = []

        a_json_str = df_fs_dist_ret.iloc[lc].to_json()
        a_a = re.sub(r"(?i)(?:\\u00[0-9a-f]{2})+", untangle_utf8, a_json_str)

        lst_travel.append(a_a)
        lat = rw['lat']
        lon = rw['lon']
        point = (lat, lon)

        #Get travel time if required
        if bln_travel_times:
            lst_travel_times = get_travel_times(base_point, point)
            lst_travel.append(lst_travel_times)

        lst_json.append(lst_travel)
        lc += 1

    return lst_json

############################################################
# untangle_utf8
############################################################
def untangle_utf8(match):
    """unicode issues...

    Keyword arguments:
    match -- json string with unicode issues...
    """
    escaped = match.group(0)                   # '\\u00e2\\u0082\\u00ac'
    hexstr = escaped.replace(r'\u00', '')      # 'e282ac'
    buffer = codecs.decode(hexstr, "hex")      # b'\xe2\x82\xac'

    try:
        return buffer.decode('utf8')           # '€'
    except UnicodeDecodeError:
        print("Could not decode buffer: %s" % buffer)


############################################################
# Get a list of travel time for the nearest stations
# at 2 different times
############################################################
def get_travel_times(start_point, end_point):

    travel_times = []
    distance_matrix_api_key = configImport["firestation.api_key"]

    # Mon 8am
    current_time = datetime.datetime.now()
    new_period1 = current_time.replace(hour=8, minute=00, second=00, microsecond=0)

    # epoch next monday
    next_monday = next_weekday(new_period1, 0).timestamp()  # 0 = Monday, 1=Tuesday, 2=Wednesday...

    current_time2 = datetime.datetime.now()
    new_period2 = current_time2.replace(hour=23, minute=00, second=00, microsecond=0)

    next_thursday = next_weekday(new_period2, 3).timestamp()  # 0 = Monday, 1=Tuesday, 2=Wednesday...
    if str(start_point).find("nan") == False or str(end_point).find("nan") == False:
        LOGGER.debug("start or end point is null %s %s", str(start_point), str(end_point))
    else:

        lst_res_mon = get_travel_time(distance_matrix_api_key, start_point, end_point, next_monday)
        # Thurs 11pm
        lst_res_thur = get_travel_time(distance_matrix_api_key, start_point, end_point, next_thursday)
        travel_times.append(lst_res_mon)
        travel_times.append(lst_res_thur)

    return travel_times


############################################################
# Get query string value from link
############################################################
def get_qs_value(url, query_string):
    """get query string from passed url query string

    Keyword arguments:
    url -- href link
    query_string -- query string key to search for
    """
    try:

        parsed = urlparse.urlparse(url)
        qs_value = urlparse.parse_qs(parsed.query)[query_string]

        for k in qs_value:
            return str(k)

    except KeyError:
        return ""
    except Exception:
        raise Exception("Error in get_qs_value - %s %s ", url, query_string)

############################################################
# Get haversine distance
############################################################
def get_haversine_dist(start_point, end_point):
    """get haversine distance betweent 2 points

    Keyword arguments:
    start_point -- start point
    end_point -- end point
    """

    dist = haversine(start_point, end_point, unit='mi')

    if math.isnan(dist):
        rv_value = 100000000
    else:
        rv_value = dist

    return rv_value


############################################################
# Lookup postcode on API to get lat/long
############################################################
def do_postcode_lookup(postcode_api_url, post_code):
    """Do API postcode lookup

    Keyword arguments:
    api_url -- URL for the API call
    post_code -- passed post code to lookup
    """
    parsed = urllib.parse.quote(post_code)
    lkp_url = postcode_api_url + str(post_code)

    resp = requests.get(url=lkp_url)
    data = resp.json()

    return data


############################################################
# Lookup travel time based on google for specified travel
# time
############################################################
def get_travel_time(api_key, start_point, end_point, dept_time):
    """Do API postcode lookup for travel time

    Keyword arguments:
    api_key -- URL for the API call
    start_point -- start point (lat,lon)
    end_point -- end point (lat,lon)
    dept_time -- departure time
    """
    ssl._create_default_https_context = ssl._create_unverified_context

    try:

        gmaps = googlemaps.Client(key=api_key)
        now = datetime.datetime.now()
        directions_result = gmaps.directions(start_point, #("52.141366,-0.479573",
                                             end_point, #"52.141366,-0.489573",
                                             mode="driving",
                                             avoid="ferries",
                                             departure_time=dept_time)
        directions_dic = defaultdict(list)

        LOGGER.debug(directions_result)

        directions_dic["distance"].append(directions_result[0]['legs'][0]['distance']['text'])
        directions_dic["duration"].append(directions_result[0]['legs'][0]['duration']['text'])
        directions_dic["time"].append(dept_time)

        return directions_dic
    except Exception:
        LOGGER.error("Error in get_travel_time - please check:" + str(exrec.data))
        raise Exception("Error in get_travel_time")

############################################################
# Get EPOCH days ahead
############################################################
def next_weekday(d, weekday):

    days_ahead = weekday - d.weekday()
    if days_ahead <= 0: # Target day already happened this week
        days_ahead += 7

    return d + datetime.timedelta(days_ahead)


############################################################
# Run
############################################################
if __name__ == "__main__":

    try:
        configImport = load_config("F:\\Programming\\python\\projects\\firestarter\\firestation_ingest.ini")

        LOG_PATH = configImport["logging.log_path"]
        LOG_FILE = configImport["logging.log_file"]
        THE_LOG = LOG_PATH + "\\" + LOG_FILE
        LOGGING_LEVEL = configImport["logging.logginglevel"]

        LEVELS = {'debug': logging.DEBUG,
                  'info': logging.INFO,
                  'warning': logging.WARNING,
                  'error': logging.ERROR,
                  'critical': logging.CRITICAL}

        # create LOGGER
        LOGGER = logging.getLogger('firestation')

        LEVEL = LEVELS.get(LOGGING_LEVEL, logging.NOTSET)
        logging.basicConfig(level=LEVEL)

        HANDLER = logging.handlers.RotatingFileHandler(THE_LOG, maxBytes=1036288, backupCount=5)
        # create FORMATTER
        FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        HANDLER.setFormatter(FORMATTER)
        LOGGER.addHandler(HANDLER)

        main()

    except  FirestationIngestError as exrec:
        LOGGER.error("Error in ingest - please check:" + str(exrec.data))
        raise Exception("Fire station ingest failed - please check")

    except Exception:
        LOGGER.error("An Exception in : MAIN :" + __name__)
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("Fire station ingest failed - please check")
