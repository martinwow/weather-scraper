# XML_scraper

A simple project that scrapes weather data from automatic weather stations and stores it for future analysis. Code is stored in a single file *main.py*.

Source of weather data is the [arso page](https://meteo.arso.gov.si/met/sl/service/), we're scraping data in the XML format.

User can specify list of measuring stations that should be monitored and for which parameters data should be retrieved.

Data is fetched every hour after starting the process. Initially, it's stored in a pandas dataframe, giving options for analytical procedures. After that, data is optionally stored into a csv file as well as a simple SQL database. Two databases are an option: sqlite and mySQL. SQLite database can be created, mySQL is a mock database.

When a database is first established, the CREATE TABLE statement is initialized. This takes into account the parameters that were specified at the beginning. Additional inputs into database are done with INSERT statement.

Since this is a simple project, each database has its own write-to function even though the procedure is nearly identical. In another iteration of the code I would wrap most of the functionality into a decorator and then specify only details for different databases.

Additional project development would entail replacing print statements with writing to a logger file, use of other data containers or alternative databases. Another option is to encapsulate the code into a class that would enable usage of messaging systems such as Redis or Kafka.