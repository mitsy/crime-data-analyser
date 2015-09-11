from goose import Goose
import re
from pyspark import SparkContext

def getTextFromUrl(url):
   """returns the url, text extracted from it and date
   """
   #time.sleep(0.5)
   text=""
   try:
      g = Goose();
      article = g.extract(url=url);
      text = article.cleaned_text;
   except:
      text = ""

   return re.sub(r'(^[ \t]+|[ \t]+(?=:))', '', text, flags=re.M).replace('\n', ' ').replace('\r', '')

#print getTextFromUrl('http://timesofindia.indiatimes.com/india/Congress-demands-Presidents-rule-in-Gujarat/articleshow/48703994.cms')

def getLocationFromText(text):
    if(text.find(":")==-1):
        return ' '
    location = text[0:text.find(":")]
    if (len(location) > 30):
        return ' '.strip()
    return location.strip().upper()

#print getLocationFromText("asdhaisd: oidufsdf")

def extractAge(text):
    pattern1 = re.compile("([0-9]+)[ |-]year|years[ |-]old")
    matchObject = pattern1.search(text)
    if (matchObject != None):
        return matchObject.group(1)
    
    pattern1 = re.compile("[A-Za-z]+, ([0-9]+), [A-Za-z]+")
    matchObject = pattern1.search(text)
    if (matchObject != None):
        return matchObject.group(1)
    
    return ""
    
#print extractAge(getTextFromUrl("http://timesofindia.indiatimes.com/city/mumbai/Rape-case-on-Sena-man/articleshow/48701818.cms"))

def fetchInputData(sc, linksFile, dataFile, crimeType):
    inputRDD = sc.textFile(linksFile)

    extractedText = inputRDD.map(lambda line: line.split('\t'))
    extractedText = extractedText.map(lambda (url, dt): (url, dt, getTextFromUrl(url)))

    extractedLocation = extractedText.map(lambda (url,dt,text): (url,dt,text,getLocationFromText(text)))
    
    outputRDD = extractedLocation.map(lambda (url,dt,text,loc): url+"||"+dt+"||"+text+"||"+loc)
    outputRDD.saveAsTextFile(dataFile)
    return extractedLocation

def ageGroup(age):
    try:
        n=int(age)
        return "" + str(n/10*10) + " - " + str(n/10*10+10)
    except:
        return "Invalid"


def readInputData(sc, dataFile):
    return sc.textFile(dataFile).map(lambda line: line.split('||'))


def processTOIData(sc, shouldFetch, linksFile, dataFile):
    if (shouldFetch):
        rapeRDD = fetchInputData(sc, linksFile, dataFile, "rape")
    rapeRDD = readInputData(sc, dataFile)
    #print rapeRDD.collect()
    
    rapeRDD = rapeRDD.map(lambda (url,dt,text,loc): (url,dt,text,loc.strip().upper(), extractAge(text)))
    
    rapeCityCounts = rapeRDD.map(lambda (url,dt,text,loc,age): (loc,1)).reduceByKey(lambda x,y: x+y)
    rapeCityCounts = rapeCityCounts.map(lambda (x,y): (y,x)).sortByKey(ascending=False).map(lambda (x,y): (y,x))
    
    rapeAgeCounts = rapeRDD.map(lambda (url,dt,text,loc,age): (age,1)).reduceByKey(lambda x,y: x+y)
    rapeAgeCounts = rapeAgeCounts.map(lambda (x,y): (y,x)).sortByKey(ascending=False).map(lambda (x,y): (y,x))
    
    rapeAgeGroups = rapeAgeCounts.map(lambda (age,counts): (ageGroup(age),counts)).reduceByKey(lambda x,y: x+y)
    rapeAgeGroups = rapeAgeGroups.map(lambda (x,y): (y,x)).sortByKey(ascending=False).map(lambda (x,y): (y,x))
    
    
    return rapeCityCounts, rapeAgeCounts, rapeAgeGroups;


def getGeonames(sc,filename,country):
    geonamesRDD = sc.textFile(filename)
    geonamesRDD = geonamesRDD.map(lambda line: line.split("\t"))
    geonamesRDD = geonamesRDD.map(lambda (geonameid, name, asciiname, alternatenames, latitude, \
                                          longitude, featureclass, featurecode, countrycode, cc2, \
                                          admin1code, admin2code, admin3code, admin4code, population, \
                                          elevation, dem, timezone, modificationdate): (asciiname.upper(), latitude, longitude, countrycode))
    indiaData = geonamesRDD.filter(lambda (asciiname, latitude, longitude, countrycode): countrycode == country)
    return indiaData


sc = SparkContext()

rapeCityCounts, rapeAgeCounts, rapeAgeGroups = processTOIData(sc, False, "rape_links.tsv", "rape_crawled.txt")
murderCityCounts, murderAgeCounts, murderAgeGroups = processTOIData(sc, False, "murder_links.tsv", "murder_crawled.txt")

#print rapeCityCounts.collect()

indiaRDD = getGeonames(sc, "allCountries.txt", "IN")

# Now join the rapeCityCounts and indiaRDD to get lat/longs for each location
indiaRDD = indiaRDD.map(lambda (name,lat,lng,code): (name, (lat,lng)))
joinedRapeRDD = rapeCityCounts.join(indiaRDD).groupByKey().map(lambda (city, grp): (city, next(iter(grp))))
joinedRapeRDD = joinedRapeRDD.map(lambda (city, (count, (lat, lng))): (city, count, lat, lng))

joinedRapeRDD = joinedRapeRDD.map(lambda (city, count, lat, lng): (count, (city, lat, lng))).sortByKey(ascending=False) \
                                            .map(lambda (count, (city, lat, lng)): (city, count, lat, lng))

#[(u'SAMASTIPUR', (1, (u'25.86077', u'85.78971'))), (u'SASARAM', (1, (u'24.94872', u'84.01693')))]

joinedMurderRDD = murderCityCounts.join(indiaRDD).groupByKey().map(lambda (city, grp): (city, next(iter(grp))))
joinedMurderRDD = joinedMurderRDD.map(lambda (city, (count, (lat, lng))): (city, count, lat, lng))
joinedMurderRDD = joinedMurderRDD.map(lambda (city, count, lat, lng): (count, (city, lat, lng))).sortByKey(ascending=False) \
                                            .map(lambda (count, (city, lat, lng)): (city, count, lat, lng))



#print "Rape locations: " + str(joinedRapeRDD.collect())
#print "Rape age groups: " + str(rapeAgeGroups.collect())
#print "Murder locations: " + joinedMurderRDD.collect()
#print "Murder age groups: " + murderAgeGroups.collect()

sc.parallelize(joinedRapeRDD.collect()).saveAsTextFile("output/rapeLocations")
sc.parallelize(rapeAgeGroups.collect()).saveAsTextFile("output/rapeAgeGroups")
sc.parallelize(joinedMurderRDD.collect()).saveAsTextFile("output/murderLocations")
sc.parallelize(murderAgeGroups.collect()).saveAsTextFile("output/murderAgeGroups")

