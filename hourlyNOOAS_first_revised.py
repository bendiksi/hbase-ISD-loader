__author__ = 'bendik'
import os
import happybase
import datetime
from time import mktime
import time
import thrift
import Geohash
import quadtree
import csv
import gzip
path ="/home/hduser/weatherData"
os.chdir(path)
stations =open("isd-history.csv",'r')
# # csv_stations = open("ghcnd-stations2.txt",'w')
os.chdir("/home/hduser/weatherData/2014")
from io import StringIO


def read_time(timeString): #read measurement
    year = int(timeString[0:4])
    month = int(timeString[4:6])
    day = int(timeString[6:8])
    hour = int(timeString[8:10])
    minutes = int(timeString[10:12])
    # print year, month, day, hour,minutes
    return str(int(mktime(datetime.datetime(year,month,day,hour,minutes).timetuple())))

def putInHbaseData(table,id,data):
    c = None
    #print 'hbase putting ', data
    while True:
        try:
            c = happybase.Connection(host='131.159.52.105',port=8080)
            #print "connected"
            break
        except thrift.transport.TTransport.TTransportException:
            print "unable to connect"
            time.sleep(10)

    if c.tables().__contains__(table):
        # dName=name.encode('utf-8')
        table = c.table(table)
        table.put(str(id),
                  data
        )
        #print 'hbase put',data

    else:
        print 'table does not exist'
    c.close()


def parse_measurement(measurement,Name):
    data = {}
    data['mt:la'] = str(float(measurement[28:34])/1000) #latitude
    data['mt:lo'] = str(float(measurement[34:41])/1000) #longitude
    data['mt:t'] = str(read_time(measurement[15:27]))	#timestamp
    data['mt:ot'] = measurement[41:46]			#observation type
    data['mt:e'] = str(int(measurement[46:51]))#elevation
    data['mt:ci'] = measurement[51:56] #callletterID
    data['m:d'] = str(int(measurement[60:63]))# #Winddirection
    data['m:dq'] = measurement[63]	#Winddirection quality
    data['m:wo'] =measurement[64] #Wind windObservationType
    data['m:f'] = str(float(measurement[65:69])/10) #windSpeed
    data['m:fq'] = measurement[69]		#windspeed quality
    data['m:ch'] = str(int(measurement[70:75])) #ceiling height
    data['m:cq'] = measurement[75]	     #ceiling Quality
    data['m:co'] = measurement[76]		#ceiling detection code
    data['m:vd'] = str(int(measurement[78:84])) #visibility distance
    data['m:vq'] = measurement[84]		#Visibility quality
    data['m:t'] = str(float(measurement[87:92])/10)	#air temperature
    data['m:tq'] = measurement[92]			#temerature quality
    data['m:p'] = str(float(measurement[100:104])/10)  # air pressure
    data['m:pq'] = measurement[104]		#air pressure quality
    data['mt:n'] = Name						#station name
    return data


    # print Year,Month,Measurement
    # for value in values:
    #     print value
def parse_station(key,station):
    data = {}
    data['m:la'] = str(float(station[6]))
    data['m:lo'] = str(float(station[7]))
    data['m:ft'] = station[9]
    data['m:tt'] = station[10]
    data['m:el'] = str(float(station[8]))
    data['m:na'] = station[2].strip(" ")
    data['m:id'] = station[0]
    data['m:WB'] = station[1]
    data['m:ha'] = key
    return data
caught_up = True




# s_cols = ['ID',' Latitude','Longitude','Elevation','Name']
#caught_up = False

parsed_stations ='ID|Lat|Lon|Elevation|Name\n'
for line in csv.reader(stations): #loop through stations
    ID = str(line[0])
    WBAN = str(line[1])
    
    print line
    try:
        stationLatitude =float(str(line[6]).strip(" "))
        stationLongitude = float(str(line[7]).strip(" "))
    except ValueError, v:
        print v, line[6],line[7]
        continue
    # Elevation = line[30:37] #feet
#    if ID == '0682640' and WBAN== '99999':
#	caught_up = True
#    if ID == '720541' and WBAN== '53806':
#	caught_up = False
#	print 'done'
#	break
    Name = line[2].strip(" ")
    path = ID+"-"+WBAN+"-2014.gz"
    hashedLocation = str(quadtree.encode(float(stationLatitude),float(stationLongitude)))
    try:
	parsed_station = parse_station(hashedLocation,line)
    except:
	print "failed to parse station: skippintg", Name
	continue
    if not caught_up:
	continue
    try:
        measurements  = gzip.open(path,'r')
       # print hashedLocation,parsed_station
        putInHbaseData('stations',hashedLocation,parsed_station)
        for measurement in measurements.readlines():#each line in the file contains readings for a month
            parsed_measurement = parse_measurement(measurement,Name)
	    if int(parsed_measurement['mt:t']) > 1396137600:
           	break
            #
            # if(stationLongitude!=lon):
            #     print "not equal longitude", stationLongitude,lon
            #
            #
            # # if(stationLatitude!=lat):
        #         print "not equal latitude", stationLatitude,parsed_measurement['lat']
        #         print "not equal latitude", stationLatitude,parsed_measurement['lat']
            # for day in parsed_month:
                # print day, parsed_month[day]
            # timestamp = str(int(day))
            # Value = parsed_month[day]
            # print parsed_measurement
  	    Id = hashedLocation+'-'+ parsed_measurement['mt:t']

                  # print "ID",Id
            # print "timestamp",parsed_measurement['time:ts']
            # print "Hashed Location",hashedLocation
            putInHbaseData('measurements',Id,parsed_measurement)
            # putInMeasurementsWithSource(Id,parsed_measurement['coordinates:lat'] ,parsed_measurement['coordinates:lon'],parsed_measurement['station:name'],'DD',parsed_measurement['measurement:DD'],parsed_measurement['time'],hashedLocation,'NOAA')

    except IOError:
        print "no such file"
        print path



# parsed_stations_io = "ID,Lat,Lon,Elevation,Name\n"+parsed_stations
# csv_stations.write(parsed_stations)
# print pd.read_csv(parsed_stations_io,dtype={'Lat':float,'Lon':float,'Elevation':float})
# stations_df =pd.read_csv(stations,sep='|',names = s_cols,dtype={' Latitude':float,'Longitude':float,'Elevation':float},index_col = 'ID')
# stations_df =pd.DataFrame.from_csv(stations,sep='|')
# stationsIterator =
# while stations_df.hasNext():
#   station = stations_df.next()

