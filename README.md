# crime-data-analyser
spark project

Steps
-----

1. The geonames data has to be downloaded:
```	
	wget http://download.geonames.org/export/dump/allCountries.zip
```
2. Unzip the file allCountries.zip
```
	unzip allCountries.zip
```	
3. Install python library goose-extractor which extracts main text from an article
```	
	easy_install goose-extractor
```
4. Run the spark program as
```
	bin/pyspark CrimeDataAnalyser.py
```
Note
----

First time when you run the file CrimeDataAnalyser.py, the second argument in processTOIData() should be set to True
```
        rapeCityCounts, rapeAgeCounts, rapeAgeGroups = processTOIData(sc, True, "rape_links.tsv", "rape_crawled.txt")
        murderCityCounts, murderAgeCounts, murderAgeGroups = processTOIData(sc, True, "murder_links.tsv", "murder_crawled.txt")
```
This ensures that the crawler fetches all the article text from the *links.tsv files. For all the subsequent runs, the second argument should be set to False.
