# AU Weather Pipeline

Welcome to my Australia weather data engineering project.

After spending 2 years working as a Data Engineer at Cognizant in London, the English weather finally broke me. I’ve since spent the last 1.5 years travelling around Australia and am now looking to settle down in a city where I can continue my career.

To refresh my data engineering skills, I decided to build a medallion architecture pipeline (Bronze → Silver → Gold) using Python and SQL Server. The goal is to compare weather patterns across Australian cities and use data to inform my decision on where to settle.

The pipeline will ingest 365 days of daily data from the Open-Meteo API for 8 Australian cities, alongside a custom city_metadata.csv dataset for enrichment and analysis.

## Tech Stack

- Python (data ingestion & orchestration)
- SQL Server Express + SSMS (data warehouse)
- Open-Meteo API (weather data source)
- CSV (city metadata)
- GitHub (version control)

## Architecture Overview

The project follows a medallion architecture:

- **Bronze**: Raw API JSON and CSV data stored as-is
- **Silver**: JSON flattened, cleaned, structured, and validated data
- **Gold**: Modelled data in a star schema for analytics

## Data Flow

Open-Meteo API + city_metadata.csv  
→ Bronze (raw ingestion via Python)  
→ Silver (cleaning, transformation, enrichment in SQL)  
→ Gold (fact and dimension tables for analysis)

## Visualisation

Data from the Gold layer is queried using Python and visualised using matplotlib to produce a 'relocation decision' dashboard comparing weather patterns across cities.

This acts as the final consumption layer of the pipeline, enabling a data-driven decision on where to settle in Australia.

## Cities

- Sydney
- Melbourne
- Brisbane
- Perth
- Adelaide
- Canberra
- Darwin
- Hobart

## Open-Meteo API Variables

The pipeline ingests daily weather data from the Open-Meteo API using the following variables:

- temperature_2m_max
- temperature_2m_min
- precipitation_sum
- precipitation_hours
- windspeed_10m_max
- weathercode
- uv_index_max
- sunshine_duration

These provide a combination of temperature, precipitation, wind, UV exposure, and sunshine metrics to support a well-rounded comparison of Australian cities.

## Notes on Data Quality

The Open-Meteo API provides relatively clean and well-structured data. To better reflect real-world data engineering scenarios, the city metadata dataset intentionally includes inconsistencies.
These issues are resolved in the Silver layer and this approach ensures the transformation layer demonstrates realistic data quality handling rather than simply passing through already clean data.

## Status

🚧 In Progress — currently building Bronze ingestion layer
