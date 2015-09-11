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
To run a fresh crawler, set the second argument in processTOIData() to True.
