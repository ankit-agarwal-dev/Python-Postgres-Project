# ETL Pipeline for I94 Immigration and US city's Temperature Data

## Project Summary
The goal of this project is to create an ETL pipeline using I94 immigration data for US and US city's temperature data to form a database that is optimized for queries on immigration events and temperature events across dates.
This database can be utilised to answer queries as follows
- Find temperature of the city on the day when a Immigrant has landed (witin US city). 
- This can also be utlised to answer questions like difference in temperature when number of people landed or departed from the US cities. 
- Another question that this database can be used is to answer trends in gender statistics of people immigrating from various countries to US.
- Time series view across dates for type of VISA issues can also be analysed through this datasets.

### Step 1: Scope the Project and Gather Data
**Scope**  
In this project, we will first load I94 code diemnsion tables with all the code used in the data set. We will then load I94 Immigration table in stage. Post stage load these records will be validated against the code table and validated records will be marked as valid. Using I94 Immigration data we will carve out Time and Person dimensions.  Next, we will load US city temperature data into the fact table. And at last we will move data in to I94 Immigration Fact table.     

**Describe and Gather Data**  
The I94 immigration data comes from the US National Tourism and Trade Office. It is provided in SAS7BDAT format which is a binary database storage format. Some relevant attributes include:

cicid = Unique Indexed column  
i94yr = 4 digit year  
i94mon = numeric month   
i94cit = 3 digit code of origin city  
i94port = 3 character code of destination USA city  
i94addr = address of city immigrant is coming for  
arrdate = arrival date in the USA (SAS date format)   
i94mode = 1 digit travel code  
depdate = departure date from the USA (SAS date format)  
i94visa = reason for immigration(1 = Business; 2 = Pleasure; 3 = Student)  
i94bir = Age of Respondent in Years  
biryear = 4 digit year of birth  
gender = immigrant sex  
airline = Airline used to arrive in U.S.  
fltno = Flight number of Airline used to arrive in U.S.  
visatype =  Class of admission legally admitting the non-immigrant to temporarily stay in U.S.   

The temperature data comes from Kaggle. It is provided in csv format. Some relevant attributes include:

AverageTemperature = average temperature  
City = city name  
Country = country name  
Latitude= latitude  
Longitude = longitude  

Following functions with in the etl.py file is used for gathering the data
```python
process_temperature_file - Processing temperature file and load data
process_i94_file - Processing Immigration data and load
```

### Step 2: Explore, Access and Clean the Data
Nest step is to clean the data. We will mark Immigration entries as valid if the airport code exist in the code value table.
Following function would be used to clean the data
```python
validate_data - Validate data from the stage table
```
### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model

Primarily a star model has been chossen for the project as this is the best model for ad-hoc analysis and queries for bulky data sets. This model contains 6 Dimensions and 2 Facts. Facts can only be joined amongst each other through Dimensions.

To start with 4 Dimensions table will be created for code dataset.
Following dimensions will be carved out of same.
- I94 Country Description (I94_country_desc_dim) = Country Code and Description 
- I94 Address Description (I94_address_desc_dim) = Address Code of US cities 
- I94 Airport Description (I94_airport_desc_dim)= Airport Description
- I94 Travel Mode         (I94_travel_mode_dim)= Travel Mode codes

After loading the dimensions, Immigration data will be loaded and stage table (I94_immigration_data_stg) and then will be validated. All validated records then will be used to create couple of more dimenions. 
Dimenions that will be created from stage tables would be
 - Time Dimension (time_dim) - Time dimension for all arrival and departure dates 
 - People Dimension (person_dim) -  Person Dimension to person related info of immigrants.

Once Dimenions are loaded Facts table would be created.
Two facts table that would be created are
 - I94 Immigration data (I94_immigration_data_fact) - I94 Immigratipn data for US
 - Temperature data (city_temperature_data_fact) - Temperature Dats for US cities 

#### 3.2 Mapping Out Data Pipelines
The pipeline steps are described below:

Step 1:  Load code Diemension table  
Step 2:  Load Immigration data into Stage table  
Step 3:  Clean the Data  
Step 4:  Create Time & person dimension from Immigration data  
Step 4:  Load Temperature Fact Table  
Step 5:  Load Immigration Fact table  

### Step 4: Run Pipelines to Model the Data
#### 4.1 Create the data model
Following code would be executed in order for populating the Dqta Model
```python
python create_tables.py  
```
Folllwing code would be used to populate the data model.
```python
python etl.py  
```

#### 4.2 Data Quality Checks
The data quality check will ensure there are adequate number of entries in each table.
Following function will check the data quality 
```python
  data_quality_check(cur, conn, table_name, Minimum_number_of_records)
```

#### 4.3 Data dictionary
**Code Dimension Tables**  
**I94_country_desc_dim**  
country_id = Country Id (PK)  
country_desc = Country  Desription  

**I94_address_desc_dim**  
address_code = Address Code (PK)  
address_desc = Address Description  

**I94_airport_desc_dim**  
airport_code = Airport Code (PK)  
airport_desc = Airport Descrption  

**I94_travel_mode_dim**  
travel_mode_cd = Travel Mode Code (PK)  
travel_mode_desc = Travel Mode Description  

**Other Dimension**    
**Time_dim**  
sas_date_value = SAS Date Value (PK)    
formatted_date = Fomatted Date (Not Null)  
year = Year in 4 digits   
month = Month in 2 digits  
day = Day in 2 digits  

**Person_Dim**  
cicid = Unique Numeric value (PK)  
gender = Gender of immigrant  
bithyear = Birth year of immigrant  
age = Age of immigrant  

**Fact Table**  
**city_temperature_data_fact**  
dt = Date of reading (PK)  
City = City of temperature reading (PK)  
Country = Country of temperature reading (PK)  
AverageTemperature = Average Temperature of city  
AverageTemperatureUncertainty = Uncertainity Factor  
Latitude = Latitude of City   
Longitude = Longtitude of City  

**I94_immigration_data_fact**    
cicid = Unique Numeric value (PK)    
i94cit = City Code    
i94res = Residential Code    
i94port = Airport Code    
arrdate = Arrival Date    
i94mode = Mode of arrival    
i94addr = Address of stay on US     
depdate = Departure Date    
i94visa = Department of state where VISA was issued    
airline = Airline     
fltno = Flight Number    
visatype = Visatype    

### Step 5: Complete Project Write Up  
**Clearly state the rationale for the choice of tools and technologies for the project**    
Python is choosen as the programming language for perfoming various ETL operations. Postgres has been choosen as the Databe as it can support quite large data and is quite flexible in terms on technolgical integration. Python & Postgres platform independence is the one of the major plus.      

**Propose how often the data should be updated and why**  
Frequency of the data update is purely dependent upon the use case, Keeping our use-case in mine (defined in Project Summary section), data can be updated on weekly basis. Since entire use casse is analytical on nature and involves hiostorical data crunching hence weekly frequency would suffice.    

**Write a description of how you would approach the problem differently under the following scenarios**  
**The data was increased by 100x.**  
If the data was increased by 100x, I would no longer process the data as a single batch job. I would perhaps do incremental updates using a tool such as Spark (Py Spark) and utlised Cloud/Hadoop Frameowrk as storage.

**The data populates a dashboard that must be updated on a daily basis by 7am every day.**  
If the data needs to populate a dashboard daily to meet an SLA then I could use a scheduling tool such as Airflow to run the ETL pipeline overnight before the schedule.

**The database needed to be accessed by 100+ people**  
If the database needed to be accessed by 100+ people, Cloud/HDFS wppuld fir for purpose as both are good enough for handling concurrency.  

### Sample Query for Analysis
Below defined is one of the sample queries that can be used to gathe the data that would be utilised to answer pur objective queries.  

**Query to get temperature of city on the day when the flight has landed**  
select td.formatted_date,ctdf.country,ctdf.city,ctdf.AverageTemperature 
from city_temperature_data_fact ctdf,
I94_immigration_data_fact iidf,
i94_airport_desc_dim iadd,
time_dim td 
where td.sas_date_value = cast(cast(iidf.arrdate as double precision) as integer) 
and td.formatted_date = ctdf.dt 
and iadd.airport_code = iidf.i94port 
and lower(iadd.airport_desc) like '%'||lower(ctdf.city)||'%';