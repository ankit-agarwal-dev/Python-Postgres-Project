""" Database SQL statement (Postgres Compatible) 
This script is to define quesries used to create/drop/query database.
"""
    
# DROP FACT AND DIMENSION TABLES QUERIES
I94_country_desc_table_drop = "drop table if exists I94_country_desc_dim;"
I94_address_desc_table_drop = "drop table if exists I94_address_desc_dim;"
I94_airport_desc_table_drop = "drop table if exists I94_airport_desc_dim;"
I94_travel_mode_table_drop = "drop table if exists I94_travel_mode_dim;"
I94_immigration_stg_table_drop = "drop table if exists I94_immigration_data_stg;"
city_temperature_table_drop = "drop table if exists city_temperature_data_fact;"
time_dim_table_drop =  "drop table if exists time_dim;"
person_dim_table_drop = "drop table if exists person_dim;"
I94_immigration_table_drop = "drop table if exists I94_immigration_data_fact;"

# CREATE FACT AND DIMENSION TABLES QUERIES
I94_country_desc_table_create = ("""create table if not exists I94_country_desc_dim
(
country_id int primary key,
country_desc varchar(250) not null
);""")

I94_address_desc_table_create = ("""create table if not exists I94_address_desc_dim
(
address_code varchar(10) primary key,
address_desc varchar(250) not null
);""")

I94_airport_desc_table_create = ("""create table if not exists I94_airport_desc_dim
(
airport_code varchar(10) primary key,
airport_desc varchar(250) not null
);""")

I94_travel_mode_table_create = ("""create table if not exists I94_travel_mode_dim
(
travel_mode_cd int primary key,
travel_mode_desc varchar(50) not null
);""")

I94_immigration_stg_table_create = ("""create table if not exists I94_immigration_data_stg
(
cicid varchar(20),
i94yr varchar(20),
i94mon varchar(20),
i94cit varchar(20),
i94res varchar(20),
i94port varchar(5),
arrdate varchar(20),
i94mode varchar(20),
i94addr varchar(5),
depdate varchar(20),
i94bir varchar(20),
i94visa varchar(20),
count varchar(20),
dtadfile varchar(10),
visapost varchar(10),
occup varchar(100),
entdepa varchar(10),
entdepd varchar(10),
entdepu varchar(10),
matflag varchar(10),
biryear varchar(20),
dtaddto varchar(10),
gender varchar(10),
insnum varchar(20),
airline varchar(20),
admnum varchar(20),
fltno varchar(20),
visatype varchar(20),
valid_yn_flag char(1) 
);""")

I94_immigration_table_create = ("""create table if not exists I94_immigration_data_fact
(
cicid integer primary key,
i94cit integer,
i94res integer,
i94port varchar(5),
arrdate integer,
i94mode integer,
i94addr varchar(5),
depdate integer,
i94visa integer,
airline varchar(20),
fltno varchar(20),
visatype varchar(10) 
);""")

time_dim_table_create = ("""create table if not exists time_dim
(
sas_date_value int primary key,
formatted_date date not null,
year int,
month int,
day varchar(20)
);""")

person_dim_table_create = ("""create table if not exists person_dim
(
cicid int primary key,
gender char(1),
bithyear int,
age int
);""")

city_temperature_table_create = ("""create table if not exists city_temperature_data_fact
(
dt date,
AverageTemperature float,
AverageTemperatureUncertainty float,
City varchar(50),
Country varchar(50),
Latitude varchar(20),
Longitude varchar(20),
PRIMARY KEY (dt,City,Country)
);""")

# INSERT RECORDS

I94_country_desc_table_insert = ("""insert into I94_country_desc_dim
(country_id,
country_desc) 
values(%s,%s) 
on conflict (country_id) do nothing;""")

I94_address_desc_table_insert = ("""insert into I94_address_desc_dim
(address_code,
address_desc) 
values(%s,%s)
on conflict (address_code) do nothing;""")

I94_airport_desc_table_insert = ("""insert into I94_airport_desc_dim
(airport_code,
airport_desc) 
values(%s,%s)
on conflict (airport_code) do nothing;""")

I94_travel_mode_table_insert = ("""insert into I94_travel_mode_dim
(travel_mode_cd,
travel_mode_desc) 
values(%s,%s)
on conflict (travel_mode_cd) do nothing;""")

I94_immigration_stg_table_insert = ("""insert into I94_immigration_data_stg
(
cicid,
i94yr,
i94mon,
i94cit,
i94res,
i94port,
arrdate,
i94mode,
i94addr,
depdate,
i94bir,
i94visa,
count,
dtadfile,
visapost,
occup,
entdepa,
entdepd,
entdepu,
matflag,
biryear,
dtaddto,
gender,
insnum,
airline,
admnum,
fltno,
visatype
)
values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);""")

time_dim_arrdate_table_insert= ("""insert into time_dim
(
sas_date_value,
formatted_date,
year,
month,
day
)
select sas_date,
formatted_date,
extract(year from formatted_date),
extract(month from formatted_date),
extract(day from formatted_date)
from
(
select distinct cast(cast(arrdate as DOUBLE PRECISION) as integer) sas_date,
TO_DATE('01-01-1960','dd-mm-yyyy') + cast(cast(arrdate as DOUBLE PRECISION) as integer) formatted_date
from I94_immigration_data_stg
where valid_yn_flag ='Y'
union
select distinct cast(cast(depdate as double precision) as integer) sas_date,
TO_DATE('01-01-1960','dd-mm-yyyy') + cast(cast(depdate as double precision) as integer) formatted_date
from I94_immigration_data_stg
where depdate <> ''
and valid_yn_flag ='Y'
) as a
ON CONFLICT DO NOTHING
;""")

I94_immigration_table_insert = ("""insert into I94_immigration_data_fact
(
cicid,
i94cit,
i94res,
i94port,
arrdate,
i94mode,
i94addr,
depdate,
i94visa,
airline,
fltno,
visatype
)
select cast(cast(cicid as double precision) as integer),
cast(cast(i94cit as double precision) as integer),
cast(cast(i94res as double precision) as integer),
i94port,
cast(cast(arrdate as double precision) as integer),
cast(cast(i94mode as double precision) as integer),
i94addr,
case when depdate ='' then null else cast(cast(depdate as double precision) as integer) end depdate,
cast(cast(i94visa as double precision) as integer),
airline,
fltno,
visatype 
from I94_immigration_data_stg 
where valid_yn_flag ='Y'
;""")

person_dim_table_insert =("""insert into person_dim
(
cicid,
gender,
bithyear,
age
)
select distinct cast(cast(cicid as DOUBLE PRECISION) as integer),
gender,
cast(cast(biryear as DOUBLE PRECISION) as integer),
cast(cast(i94bir as DOUBLE PRECISION) as integer)
from I94_immigration_data_stg
where valid_yn_flag ='Y'
ON CONFLICT (cicid) do update set age = EXCLUDED.age;
;""")

I94_valid_port_update = ("""update I94_immigration_data_stg
set valid_yn_flag = 'Y'
where i94port in (select distinct airport_code from I94_airport_desc_dim)
;""")

city_temperature_table_insert = ("""insert into city_temperature_data_fact
(
dt,
AverageTemperature,
AverageTemperatureUncertainty,
City,
Country,
Latitude,
Longitude
)
values(%s, %s, %s, %s, %s, %s, %s
);""")

# QUERY LISTS
clean_table_queries = [I94_valid_port_update]

populate_table_queries = [time_dim_arrdate_table_insert, person_dim_table_insert, I94_immigration_table_insert]

create_table_queries = [I94_country_desc_table_create, I94_address_desc_table_create, I94_airport_desc_table_create, I94_travel_mode_table_create, time_dim_table_create, I94_immigration_stg_table_create, time_dim_table_create, person_dim_table_create, city_temperature_table_create, I94_immigration_table_create]

drop_table_queries = [I94_country_desc_table_drop, I94_address_desc_table_drop, I94_airport_desc_table_drop, I94_travel_mode_table_drop, time_dim_table_drop, I94_immigration_stg_table_drop, time_dim_table_drop, person_dim_table_drop, city_temperature_table_drop, I94_immigration_table_drop]
