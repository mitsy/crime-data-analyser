# crime-data-analyser
spark project

Steps
-----

1. The geonames data has to be downloaded:
	wget http://download.geonames.org/export/dump/allCountries.zip

2. Unzip the file allCountries.zip

3. easy_install goose-extractor

4. Run the spark program as 
	bin/pyspark CrimeDataAnalyser.py
