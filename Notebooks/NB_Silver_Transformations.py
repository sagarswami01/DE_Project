# Databricks notebook source
#Install Required Packages
%pip install geopy
%pip install h3

# COMMAND ----------

#connection to blob storage
storage_account_name = "sagarhackathon"
container_name = "bronze"
storage_account_key = dbutils.secrets.get(scope="sagaradbscope", key="az-storage-key")
# api_access_token = dbutils.secrets.get(scope="sagaradbscope", key="api-access-token")

spark.conf.set(
    "fs.azure.account.key.sagarhackathon.blob.core.windows.net",storage_account_key)

# COMMAND ----------

#reading processed raw datasets from bronze
business_df = spark.read.format("delta").load("wasbs://bronze@sagarhackathon.blob.core.windows.net/processed_data/business_location_data").distinct()

police_df = spark.read.format("delta").load("wasbs://bronze@sagarhackathon.blob.core.windows.net/processed_data/police_dept_data").distinct()

# COMMAND ----------

from pyspark.sql.functions import date_format, to_timestamp, col, when, lower, to_date, datediff, hour, minute, unix_timestamp

#handling formats and datatypes of date and timestamp columns
bus_df = business_df.withColumn('formatted_data_as_of', to_timestamp(date_format(col('data_as_of'), 'yyyy-MM-dd HH:mm:ss'))).withColumn('formatted_data_loaded_at', to_timestamp(date_format(col('data_loaded_at'), 'yyyy-MM-dd HH:mm:ss'))).withColumn('formatted_dba_start_date', to_timestamp(date_format(col('dba_start_date'), 'yyyy-MM-dd HH:mm:ss'))).withColumn('formatted_dba_end_date', to_timestamp(date_format(col('dba_end_date'), 'yyyy-MM-dd HH:mm:ss'))).withColumn('formatted_location_start_date', to_timestamp(date_format(col('location_start_date'), 'yyyy-MM-dd HH:mm:ss'))).withColumn('formatted_location_end_date', to_timestamp(date_format(col('location_end_date'), 'yyyy-MM-dd HH:mm:ss'))).withColumn('business_is_active', when(col('formatted_location_end_date').isNotNull(), False).otherwise(True)).drop('data_as_of','data_loaded_at','dba_start_date','dba_end_date','location_start_date','location_end_date')
bus_df = bus_df.withColumn('AdministrativelyClosed', when(when(col('administratively_closed').isNotNull(), lower(col('administratively_closed'))).contains("admin") | when(col('administratively_closed').isNotNull(), lower(col('administratively_closed'))).contains("closed"), True).otherwise(False))

# COMMAND ----------

df_police = police_df.withColumn('formatted_incident_date', to_date(date_format(col('incident_date'), 'yyyy-MM-dd HH:mm:ss'))).withColumn('formatted_incident_datetime', to_timestamp(date_format(col('incident_datetime'), 'yyyy-MM-dd HH:mm:ss'))).withColumn('formatted_report_datetime', to_timestamp(date_format(col('report_datetime'), 'yyyy-MM-dd HH:mm:ss'))).drop('incident_date','incident_datetime','report_datetime')
df_police = df_police.withColumn('time_taken_to_report_hrs', datediff(col('formatted_report_datetime'), col('formatted_incident_datetime')))
df_police = df_police.drop('point')

# COMMAND ----------

bus_df = bus_df.dropna(subset=['latitude', 'longitude'])
df_police = df_police.dropna(subset=['latitude', 'longitude'])
df_police = df_police.withColumn("lat_float", df_police["latitude"].cast("double")).withColumn("lon_float", df_police["longitude"].cast("double")).drop("latitude", "longitude")
df_police = df_police.withColumnRenamed("lat_float", "latitude").withColumnRenamed("lon_float", "longitude")

# COMMAND ----------

bus_df = bus_df.withColumnRenamed('latitude','business_lat').withColumnRenamed('longitude','business_lon')
df_police = df_police.withColumnRenamed('latitude','police_lat').withColumnRenamed('longitude','police_lon')

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

bus_common = bus_df.select('business_lat','business_lon','business_zip').distinct()
window_spec = Window.orderBy("business_zip")
common_tbl_bus = bus_common.withColumn('uniqueid', row_number().over(window_spec))
common_tbl_bus = common_tbl_bus.filter(col('business_zip').startswith("9"))
pol_common = df_police.select('police_lat','police_lon').distinct()

# COMMAND ----------

from geopy.distance import geodesic

#function to calculate distance betweeen location coordinates
def calculate_distance(business_lat, business_long, incident_lat, incident_long):

  business_coords = (business_lat, business_long)
  incident_coords = (incident_lat, incident_long)

  distance = geodesic(business_coords, incident_coords).km
  return distance


# COMMAND ----------

from pyspark.sql.functions import lit,udf, col, broadcast
from pyspark.sql.types import StringType
import h3

#Using H3 expressions to create H3 indexes for optimized join performance
H3_RESOLUTION = 9

def lat_lon_to_h3(lat, lon, resolution):
    return h3.geo_to_h3(lat, lon, resolution)

h3_udf = udf(lat_lon_to_h3, StringType())

# Add H3 index columns based on latitude and longitude
common_tbl_bus = common_tbl_bus.withColumn(
    "h3_index", h3_udf(col("business_lat"), col("business_lon"), lit(H3_RESOLUTION))
)

# Broadcast the second dataset to avoid shuffles during the join
broadcast_common_tbl = broadcast(pol_common).withColumn(
    "h3_index", h3_udf(col("police_lat"), col("police_lon"), lit(H3_RESOLUTION))
)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from geopy.distance import geodesic

calculate_distance_udf = udf(calculate_distance, DoubleType())

joined_df = common_tbl_bus.join(
    broadcast_common_tbl,
    common_tbl_bus["h3_index"] == broadcast_common_tbl["h3_index"],
    how="inner"
)


distance_df = joined_df.withColumn(
    "distance_km",
    calculate_distance_udf(
        col("business_lat"), col("business_lon"),
        col("police_lat"), col("police_lon")
    )
)

window_spec = Window.partitionBy("police_lat", "police_lon").orderBy("distance_km")
distance_df = distance_df.withColumn("row_num", row_number().over(window_spec))
closest_records_df = distance_df.filter(col("row_num") == 1).drop("row_num")

# COMMAND ----------

region_map = closest_records_df.drop('uniqueid','h3_index','distance_km').withColumnRenamed('police_lat','latitude').withColumnRenamed('police_lon','longitude')

# COMMAND ----------

pol_map = region_map.select('latitude','longitude','business_zip').distinct()

# COMMAND ----------

final_pol = df_police.join(pol_map, (col('police_lat') == col('latitude')) & (col('police_lon') == col('longitude')), 'left').drop('latitude','longitude','business_lat','business_lon').distinct()

# COMMAND ----------

#Renaming columns as per business names
inc_col_map = {"analysis_neighborhood" : "Analysis_Neighborhood","cad_number" : "CAD_Number","cnn" : "CNN","filed_online" : "Filed_Online","incident_category" : "Incident_Category","incident_code" : "Incident_Code","incident_day_of_week" : "Incident_Day_of_Week","incident_description" : "Incident_Description","incident_id" : "Incident_ID","incident_number" : "Incident_Number","incident_subcategory" : "Incident_Subcategory","incident_time" : "Incident_Time","incident_year" : "Incident_Year","intersection" : "Intersection","police_district" : "Police_District","report_type_code" : "Report_Type_Code","report_type_description" : "Report_Type_Description","resolution" : "Resolution","row_id" : "Row_ID","supervisor_district" : "Supervisor_District","supervisor_district_2012" : "Supervisor_District_2012","formatted_incident_date" : "Incident_Date","formatted_incident_datetime" : "Incident_Datetime","formatted_report_datetime" : "Report_Datetime","time_taken_to_report_hrs" : "Time_Taken_to_Report","police_lat" : "Incident_Latitude","police_lon" : "Incident_Longitude","business_zip" : "Incident_ZipCode"}

for old_name, new_name in inc_col_map.items():
    final_pol = final_pol.withColumnRenamed(old_name, new_name)

# COMMAND ----------

#Renaming columns as per business names
bus_df = bus_df.drop('administratively_closed')
bus_col_map = {"business_corridor" : "Business_Corridor","business_zip" : "Business_Zipcode","certificate_number" : "Business_Account_Number","city" : "City","dba_name" : "Doing_Business_Name","full_business_address" : "Street_Address","lic" : "LIC_Code","lic_code_description" : "LIC_Code_Description","lic_code_descriptions_list" : "LIC_Code_Description_List","mail_city" : "Mail_City","mail_state" : "Mail_State","mail_zipcode" : "Mail_Zipcode","mailing_address_1" : "Mail_Address","naic_code" : "NAICS_Code","naic_code_description" : "NAICS_Code_Description","naics_code_descriptions_list" : "NAICS_Code_Descriptions_List","neighborhoods_analysis_boundaries" : "Neighborhoods_Analysis_Boundaries","ownership_name" : "Ownership_Name","parking_tax" : "Parking_Tax","state" : "State","supervisor_district" : "Supervisor_District","transient_occupancy_tax" : "Transient_Occupancy_Tax","ttxid" : "Location_ID","uniqueid" : "Unique_ID","business_lat" : "Business_Latitude","business_lon" : "Business_Longitude","formatted_data_as_of" : "Source_Update_Date","formatted_data_loaded_at" : "Data_Available_Date","formatted_dba_start_date" : "Business_Start_Date","formatted_dba_end_date" : "Business_End_Date","formatted_location_start_date" : "Location_Start_Date","formatted_location_end_date" : "Location_End_Date","business_is_active" : "Business_Is_Active","AdministrativelyClosed" : "Administratively_Closed"}

for old_name, new_name in bus_col_map.items():
    bus_df = bus_df.withColumnRenamed(old_name, new_name)

# COMMAND ----------

#Write processed business data to Silver Layer
bus_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("wasbs://silver@sagarhackathon.blob.core.windows.net/business_data")

# COMMAND ----------

#Write processed incident data to Silver Layer
final_pol.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save("wasbs://silver@sagarhackathon.blob.core.windows.net/incidents_data")
