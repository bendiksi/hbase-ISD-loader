uthor__ = 'bendik'
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



def putInMeasurementsWithSource(id,lat,lon,name,measurementType,value,timestamp,hashedLocation,source):
    c = None
    print 'hbase putting ',id ,str(lon),str(lat), name, measurementType, value, timestamp
    while True:
        try:
            c = happybase.Connection(host='131.159.52.104',port=8080)
            print "connected"
            break
        except thrift.transport.TTransport.TTransportException:
            print "unable to connect"
            time.sleep(10)

    if c.tables().__contains__('measurements'):
        dName=name.encode('utf-8')
        table = c.table('measurements')
        table.put(str(id),
                  {'coordinates:lon': str(lon),
                  'coordinates:lat':str(lat),
                  'station:name':dName,
                  'source:name':source,
                  'measurement:'+measurementType: value,
                  'coordinates:hash':hashedLocation,
                  'time:'+measurementType: time})
        print 'hbase put',id ,str(lon),str(lat), dName, measurementType, value, time
    else:
        print 'table does not exist'
    c.close()

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
    data['coordinates:lat'] = str(float(measurement[28:34])/1000)
    data['coordinates:lon'] = str(float(measurement[34:41])/1000)
    data['time:ts'] = str(read_time(measurement[15:27]))
    data['station:observationType'] = measurement[41:46]
    data['coordinates:elevation'] = str(int(measurement[46:51]))
    data['station:callLetterID'] = measurement[51:56]
    data['measurement:DD'] = str(int(measurement[60:63]))
    data['measurement:DDQuality'] = measurement[63]
    data['measurement:windObservationType'] =measurement[64]
    data['measurement:FF'] = str(float(measurement[65:69])/10)
    data['measurement:FFQuality'] = measurement[69]
    data['measurement:ceilingHeight'] = str(int(measurement[70:75]))
    data['measurement:ceilingQuality'] = measurement[75]
    data['measurement:ceilingDetCode'] = measurement[76]
    data['measurement:visibilityDistance'] = str(int(measurement[78:84]))
    data['measurement:visibility quality'] = measurement[84]
    data['measurement:temp'] = str(float(measurement[87:92])/10)
    data['measurement:tempQuality'] = measurement[92]
    data['measurement:pressure'] = str(float(measurement[100:104])/10)
    data['measurement:pressureQuality'] = measurement[104]
    data['source:name'] = 'NOOA'
    data['station:name'] = Name
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

#def parse_station(key,station):
 #   data = {}
  #  data['coordinates:lat'] = str(float(station[6]))
 #   data['coordinates:lon'] = str(float(station[7]))
 #   data['from_time:ts'] = station[9]
 #   data['to_time:ts'] = station[10]
 #   data['coordinates:elevation'] = str(float(station[8]))
 #   data['name:name'] = station[2].strip(" ")
 #   data['name:id'] = station[0]
 #   data['name:WBAN'] = station[1]
 #   data['name:hash'] = key
 #   return data
caught_up = True 

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
    if ID == '999999' and WBAN== '03037':
       caught_up = True
       
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
            if int(parsed_measurement['time:ts']) > 1391212740:
                break;
	    

            # if int(parsed_measurement['mt:t']) > 1396224000:
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
            Id = hashedLocation+'-'+ parsed_measurement['time:ts']
            data = {}
            data['m:m']=str(measurement)
                  # print "ID",Id
            # print "timestamp",parsed_measurement['time:ts']
            # print "Hashed Location",hashedLocation
            putInHbaseData('measurements2',Id,data)
            # putInMeasurementsWithSource(Id,parsed_measurement['coordinates:lat'] ,parsed_measurement['coordinates:lon'],parsed_measurement['station:name'],'DD',parsed_measurement['measurement:DD'],parsed_measurement['time'],hashedLocation,'NOAA')

    except IOError:
        print "no such file"
        print path


