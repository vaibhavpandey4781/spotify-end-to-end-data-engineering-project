# Spotify End-to-End Data Engineering Project
### Introduction

This project demonstrates the design and implementation of an end-to-end data engineering pipeline using Spotify data. It automates the process of extracting, transforming, and analyzing music-related data from the Spotify API, leveraging modern cloud technologies.
The pipeline is built to replicate real-world ETL (Extract, Transform, Load) workflows and highlights important data engineering concepts such as data ingestion, storage, processing, cataloging, and querying.

### Project Workflow

Data Extraction:
Connect to Spotify Web API using Python & Spotipy.
Extract playlist, track, album, and artist data.

Raw Data Storage (Landing Zone):
Store extracted data as JSON/CSV files in Amazon S3 (raw zone).

Processing & Transformation:
Use AWS Lambda to trigger transformations.
Convert data into structured formats (CSV/Parquet).
Store cleaned data in Amazon S3 (processed zone).

Data Catalog & Querying:
Create a schema using AWS Glue Data Catalog.
Query data interactively with Amazon Athena.

Visualization (Optional):
Build dashboards using Power BI or Tableau.

### Tech Stack

Programming: Python (Spotipy, Pandas, Boto3)
Cloud: AWS S3, AWS Lambda, AWS Glue, Amazon Athena
Version Control: GitHub
Visualization (optional): Power BI / Tableau

### Objectives

Automate data ingestion from Spotify.
Build a scalable & modular data pipeline.
Store both raw and transformed data in the cloud.
Enable querying and insights generation.
Showcase practical data engineering skills.
