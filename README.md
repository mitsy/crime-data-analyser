# crime-data-analyser
spark project

Steps
-----
1. Run the below command to clone the project:
```
	git clone https://github.com/mitsy/crime-data-analyser.git
```	
With this, we are in ~/crime-data-analyser directory.

2. The geonames data has to be downloaded:
```	
	wget http://download.geonames.org/export/dump/allCountries.zip
```

3. Unzip the file allCountries.zip
```
	unzip allCountries.zip
```

4. Install python library goose-extractor which extracts main text from an article
```	
	easy_install goose-extractor
```
5. Run the spark program as
```
	bin/pyspark CrimeDataAnalyser.py
```

This will create an **output** directory inside crime-data-analyser/ which will have **rapeLocations**, **rapeAgeGroups** and **murderLocations** and **murderAgeGroups** folders. Check the part files inside these folders.

Note
----
To run a fresh crawler, set the second argument in processTOIData() in CrimeDataAnalyser.py to True. By default, it is set to False as the rape and murder crawled files are already present in this repository.
