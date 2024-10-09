# Airports Data Pipeline Expansion To Data Lakehouse Hands-On Lab

This web-based ELT application curates [OurAirports](https://ourairports.com/data/) data for a DDLH PoC or an enablement activity using Dell Data Lakehouse hands-on lab environment. It can also work in any other environment if the requirements for the app are met. The application does the following in a nutshell:

- Downloads, extracts and loads the world's airports  data to a PostgreSQL database
- Uploads the world's airports data including ever-changing runway data to an object storage to be used by hive external tables
- Dynamically creates catalog definitions for PostgreSQL, Hive and Iceberg connectors
- Create DDLH cluster configuration for Table Scan Redirection and Materialized Views
- Creates roles with necessary privileges and assign those roles to respective system and service users
- Creates scripts for PostgreSQL, Hive, Iceberg catalogs as well federated queries across these catalogs. These queries enable merging mostly static data in PostgreSQL database with frequently updated runway data in Hive external tables
- Provide scripts for migration from Hive tables to Iceberg tables 
- Create View and Materialized View scripts to be used for further visualization in PowerBi

### Demo/PoC Scenario

SuperCity Airport has a database of tables containing information on worldwide airports. Their business applications rely on it for information such as geolocation and communication details. Even though crucial for inter-airport operations, the data in these database tables rarely changes. Runways’ status, however, changes very frequently depending on different weather and runway conditions at remote airports and is provided in the form of logs for the airport to pull in. Inter-airport operations should combine this data with the information in database tables to prevent delays in advance, reduce risks, and plan for future logistics activities.

![Scneario](/site/static/pic/data-pipeline.png)

### Some Screenshots From the App

> Login using DDLH user credentials for proxy authentication
>
> ![Login](/site/static/pic/airports-pipeline-login.png)

> Create tables in PostgreSQL database
>
> ![Create tables](/site/static/pic/airports-pipeline-create-tables.png)  

> Create catalog definitions, SQL scrtipts and roles
>
> ![Definitions](/site/static/pic/airports-pipeline-definitions.png)  

> Access definitions and scripts
>
> ![Scripts](/site/static/pic/airports-pipeline-scripts.png)

> Iceberg script
>
> ![Scripts](/site/static/pic/airports-pipeline-iceberg-catalog-script.png)

### Requirements
- Lakehouse Version 1.0+
- PostgreSQL database Version 12+
- Windows Server 2022, Windows 11 (might work with other Windows flavours and versions but untested)
- Python for Windows version 3.11+ (might work with earlier versions but untested)

### Installation in Dell Data Lakehouse Hands-on Lab Environment

1. Install [Python for Windows](https://www.python.org/downloads) under drive "E"
   - Select "Add Python to environment variables" in Advanced Options
   - Select "Disable Windows path limit" in the last step
2. Install [Git for Windows](https://git-scm.com/download/win) under drive "E"
3. Switch into "E:", create "programs" folder and clone this repository under "programs" directory
```console
E:  
mkdir programs  
cd programs  
git clone https://github.com/baris-saltik/airports-pipeline  
```
4. Switch into airports-pipeline directory, create a virtual Python environment named ".venv" and activate that environment.
```console
cd airports-pipeline  
python -m venv .venv  
.venv\Scripts\activate.bat  
```
5. Install required Python packages
```console
python -m pip install -r Requirements.txt
```
6. Launch the application
```console
python airports.py
```
7. Open up a web browser and log into the app by using DDLH credentials at the following address.
```console
https://127.0.0.1:5000
```