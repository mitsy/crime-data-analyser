# crime-data-analyser
A spark project to analyse newspaper TimesOfIndia data, and extract the locations where crimes like rape and murder are most prominent along with the victim demographics.

Steps
-----
* Run the below command to clone the project:
```
	git clone https://github.com/mitsy/crime-data-analyser.git
```

* Change into the below directory:
```
	cd crime-data-analyser/
```

* The geonames data has to be downloaded:
```	
	wget http://download.geonames.org/export/dump/allCountries.zip
```

* Unzip the file allCountries.zip
```
	unzip allCountries.zip
```

* Install python library goose-extractor which extracts main text from an article
```	
	easy_install goose-extractor
```

* Run the spark program as
```
	bin/pyspark CrimeDataAnalyser.py
```

This will create an **output** directory inside crime-data-analyser/ which will have **rapeLocations**, **rapeAgeGroups**, **murderLocations** and **murderAgeGroups** folders. Check the part files inside these folders.

Note
----
To run a fresh crawl, set the second argument in **processTOIData()** in CrimeDataAnalyser.py to **True**. By default, it is set to **False** as the rape and murder crawled files are already present in this repository.
