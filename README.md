# AU Weather Pipeline

Welcome to my Australia weather data engineering project.

After spending 2 years working as a Data Engineer at Cognizant in London, the English weather finally broke me. I’ve since spent the last 1.5 years travelling around Australia and am now looking to settle down in a city where I can continue my career.

To refresh my data engineering skills, I decided to build an end to end medallion architecture pipeline (Bronze → Silver → Gold) using Python and SQL Server. The goal is to compare weather patterns across Australian cities and use data to inform my decision on where to settle.

The pipeline ingests 365 days of daily data from the Open-Meteo API for 8 Australian cities, alongside a custom city_metadata.csv dataset for enrichment and analysis.

## Tech Stack

- Python (data ingestion & orchestration)
- SQL Server Express + SSMS (data warehouse)
- Open-Meteo API (weather data source)
- CSV (city metadata)
- GitHub (version control)
- pyodbc (Python --> SQL Server connectivity layer)

## Architecture Overview

The project follows a medallion architecture:

- **Bronze**: Raw API JSON and CSV data stored as-is
- **Silver**: JSON flattened, cleaned, structured, and validated data
- **Gold**: Star schemamodel optimised for analytical queries
- **Analytics**: Wide, aggregated table for dashboard consumption

## Pipeline features

- Batch based ingestion using a unique `batch_id` for traceability
- Ingestion logging with status (STARTED/SUCCESS/FAILED)
- Seperation of ingestion logic (Python) and transformation logic (SQL)
- API data is ingested via Python and stored as raw JSON file - 1 row per city
- Stored procedure based CSV ingestion with staging tables
- Inempotent loads using TRUNCATE + INSERT

## Data Flow

→ Sources (Open-Meteo API + city_metadata.csv)  
→ Bronze (raw ingestion via Python)  
→ Silver (cleaning, transformation, enrichment in SQL)  
→ Gold (fact and dimension tables for analysis)  
→ Analytics (wide table)  
→ Dashboard (python visualisation)

## Tech Stack

- Python (data ingestion & orchestration)
- SQL Server Express + SSMS (data warehouse)
- Open-Meteo API (weather data source)
- CSV (city metadata)
- GitHub (version control)
- pyodbc (Python --> SQL Server connectivity layer)

## Analytics/Presentation Layer

The final layer of the pipeline is a **presentation (wide) table** designed for analysis and visualisation via Python and matplotlib/seaborn libraries to produce a 'relocation decision' dashboard comparing weather patterns across cities.

- Aggregates data from the Gold star schema
- Produces 1 row per city
- Pre computes key metrics and comparison scores
- Seperates business logic from the dashboad layer, esnuring metrics are resusable across tools and the data model remains clean and maintanable

This acts as the final consumption layer of the pipeline, enabling a data-driven decision on where to settle in Australia.

## Key Design Decisions

* **Medallion Architecture**  
  Chosen to clearly seperate raw ingestion, transformation, and analytical layers to closely resemble modern architecture and practices
* **Batch Based Processing (batch_id)**  
  Ensures reproducibility, traceablility, and controlled data loads
* **Filtering to Latest Batch in Silver**  
  Produces a clean, deduplicated dataset aligned with table grain
* **Composite Keys in Silver**  
  Enforces correct grain (1 row per city per day)
* **Star Schema in Gold**  
  Improves query performance and simplifies analytical queries
* **Presentation Layer (Wide Table)**  
  Seperates business logic from visualisation, making the dashboard simpler and reusable

## Cities Analysed

Sydney | Melbourne | Brisbane | Perth | Adelaide | Canberra | Darwin | Hobart

## Open-Meteo API Variables

The pipeline ingests daily weather data from the Open-Meteo API using the following variables:

- temperature_2m_max
- temperature_2m_min
- precipitation_sum
- precipitation_hours
- windspeed_10m_max
- weathercode
- sunshine_duration

These provide a combination of temperature, precipitation, wind, and sunshine metrics to support a well-rounded comparison of Australian cities.

## Notes on Data Quality

The Open-Meteo API provides relatively clean and structured data. To better reflect real world data engineering scenarios, the city metadata dataset intentionally includes inconsistencies.
These issues are resolved in the Silver layer and this approach ensures the transformation layer demonstrates realistic data quality handling rather than simply passing through already clean data.

## Status

🚧 In Progress — Bronze ingestion layer complete, Silver transformation layer complete, Gold and Analytics layer in progress.
