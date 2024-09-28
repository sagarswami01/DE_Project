# Azure Cloud Data Engineering: API-Driven ETL/ELT Pipelines

## Project Overview

This project uses two datasets [Business Data](https://data.sfgov.org/Economy-and-Community/Registered-Business-Locations-San-Francisco/g8m3-pdis/about_data) and [Incident Reports](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783/about_data) from San Francisco government's public datasets website to provide following data-driven insights:
1. Hotspot Analysis: Crime rate in the vicinity of business locations
2. Correlation Analysis: Types of business more prone to specific types of crimes
3. Geospatial Analysis: Heatmaps and buffer analysis
4. Crisis Management: Plan emergency situations with nearby emergency services data

**Note:** This project only prepares and processes data to serve downstream applications, Data Science teams, and Business Owners for further analysis.

## Technical Architecture
<img width="655" alt="architecture" src="https://github.com/user-attachments/assets/d12ba796-8855-4076-a80d-e2d655c0a717">


## Key Technologies

Major tools/technologies used in this project:
1. Python (For API calls, transformations)
2. Azure Data Factory/Databricks (For orchestration)
3. Azure Blob Storage/Data Lake (For data storage)
4. Azure SQL (For metadata and logs)
5. Databricks (For loading and processing data)
6. Pandas, Pyspark (For transformations)
7. REST APIs (To fetch data)

## Datasets

Datasets used: [Business Data](https://data.sfgov.org/Economy-and-Community/Registered-Business-Locations-San-Francisco/g8m3-pdis/about_data) and [Incident Reports](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783/about_data)
Business Data: This dataset includes the locations of businesses that pay taxes to the City and County of San Francisco. Each registered business may have multiple locations and each location is a single row.
Incident Reports: This dataset includes incident reports that have been filed as of January 1, 2018. These reports are filed by officers or self-reported by members of the public using SFPD’s online reporting system.
If you would like more information, you can visit the website.

Data Source Refresh Schedule:
Business Data - 04:11 AM PST
Incidents Data - 10:00 AM PST
**Note:** As refresh schedules are in PST, in this project the data fetch job is scheduled at 11:00 PM IST to fetch both datasets together considering reasonable buffer time.

Data Fetch Mechanism:
API authentication is not required as data is fetched using SODA API 2.0 endpoints (No restrictions on throttling limits)
However, if you need to implement authentication refer [developer's website](https://data.sfgov.org/profile/edit/developer_settings) to generate your API tokens and secrets.



