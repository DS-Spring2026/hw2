# HW2

In this assignment, you will be using Databricks on Google Cloud Platform (GCP), creating two tables with data from the yellow cab NYC Taxi Dataset [NYC Taxi Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), and then running a set of SQL queries against that table. 

There will be two parts to the analytical portion of the assigment. In the first part you will run a set of SQL queries in Databricks designed to reflect different workload patterns (for example, varying selectivity, aggregation/join complexity, and effective data scanned) in distributed data work.

In the second part, you will then import this same data into BigQuery and run the same queries against that table. You will measure the runtimes of each and compare the performance of the SQL workload on BigQuery and Databricks.

**As a note, this lab will likely trigger some pop-up windows prompting login, and if you are using Safari pop-ups are sometimes blocked. Just check the URL box to see if this is happening and you can open pop-ups from there, or use another browser like Firefox or Chrome.**
Additionally, in this process you may be redirected and asked to login by selecting your google account a few times. That is normal, just click through that process, and allow Databricks the permissions it requests for. 

# Submission 
## Where to Submit
Accept the Github classroom link for HW2 posted on Canvas. This will create a Github repo for you and please push your writeup to that repo.  

## Writeup Requirements 
Include the following information:
1. Performance information for each query. In part 2 include the performance for both BigQuery and Databricks.
2. Some analysis of this data. What queries seem to have the same performance accross platforms? What have different? For those that are different, do you have hypotheses on what is making the performance different?
3. Any feedback you have on the assignment, relative difficulty, or other information.

# Assignment Description 
## Prepare Databricks
Go to the link for [Databricks on GCP](https://cloud.google.com/databricks?hl=en) and then click "Try on Marketplace".

<img width="1098" alt="image" src="https://github.com/user-attachments/assets/2706b2cf-87ac-4fe7-bc2c-45d27c601010" />

Then click the checkbox and `agree and continue` in the bottom right.

<img width="563" alt="image" src="https://github.com/user-attachments/assets/86fddc7c-c853-423a-ad8f-d1d8c66808a8" />

Then click the "Databricks" box and then on the next page click "Subscribe". 
<img width="808" alt="image" src="https://github.com/user-attachments/assets/79862dbd-cab7-48b9-8644-cbb76e9879a7" />

<img width="754" alt="image" src="https://github.com/user-attachments/assets/3c2a7742-11c2-4469-ba7c-90437c295ef2" />



After hitting subscribe, you'll be brought to a new page. Scroll down, under "Purchase details" select your billing account, then click the box at the bottom and click "Subscribe".
<img width="940" alt="image" src="https://github.com/user-attachments/assets/6b6766c3-a5db-44a5-b3ce-ef3d2da46117" />


After this, we're going to need to make an account with Databricks through their portal. Click the "Sign Up With Databricks" button. 

<img width="1507" alt="image" src="https://github.com/user-attachments/assets/7f288f1f-b9f2-423e-b7ce-29ba88c844dd" />

You want to create an organization name (this can be anything) and then click Login with Google and then select the Google account you're using for the Google Cloud Platform setup. This will process for a little bit, and it's possible it may require you to refresh the pop-up window and re-enter the information. Once it's done the pop-up window should close and you can go back to the other GCP tab you had open. 

<img width="1214" alt="image" src="https://github.com/user-attachments/assets/0dc1a461-c2c3-4998-9fb7-dbd62c474b64" />

On that tab you should see that it has changed (or refresh it first) to have a button that says "Manage on Provider." Click this button to be taken to the Databricks site. 

<img width="1509" alt="image" src="https://github.com/user-attachments/assets/43e420e2-7297-41e6-adff-248f34f2a3ad" />

Once on the databricks site, you can once again login with Google. It will prompt you select a subscription plan. This comes with a 14-day free trial, which will be enough to cover all the usage we need. 

<img width="1511" alt="image" src="https://github.com/user-attachments/assets/a2cdf245-5dce-46e7-8300-3582b42d6fea" />

<img width="1512" alt="image" src="https://github.com/user-attachments/assets/9d236cd8-ff22-49d4-856f-8326c50c4c50" />

## Create Workspace
Next, we're going to go back to our GCP page. In the upper part of the screen, you'll see a box with your project name. In the example image, there is a red-box drawn around this. Click that box and you'll be shown an overlay that lists the project and it's project ID. Copy this project ID as it will be needed to create a Databricks Workspace. This ID is also labelled with a red box. in the second image below.

<img width="881" alt="image" src="https://github.com/user-attachments/assets/57b27960-dd24-43b3-9531-3f9f1e30df84" />

<img width="762" alt="image" src="https://github.com/user-attachments/assets/57905d80-84f1-4443-9298-25c40831679c" />

Once you're logged in, you'll see a workspaces tab on the left hand nav bar. Select that and select "Create Workspace." You will be brought to a page which asks you to give the workspace a name ***make this data342 to fit with the pre-written SQL workload***, pick a GCP region (I chose us-east-1), and paste the project ID you copied in the previous step. Then click "Create workspace."

<img width="1511" alt="image" src="https://github.com/user-attachments/assets/de404b8d-1235-45bf-bdef-42256f265146" />

<img width="1029" alt="image" src="https://github.com/user-attachments/assets/40afaa0e-f445-404a-960b-77cf0df49347" />

## Create Table
Download the yellow-cab Parquet data files from January 2022 to December 2025 (total of 48 files) and merge them into a single file using the below script (total file size would be around 3GB). Upload the merged Parquet file into a single table by first creating a volume and uploading the file to that volume, then reading the data from the volume and executing SQL like the example below.

```sql
CREATE TABLE nyc_taxi_22_25
USING PARQUET
LOCATION '/Volumes/<catalog>/<schema>/<volume>/<your_file>.pq'; 
```
Please also download the csv file "Taxi Zone Lookup Table (CSV)" mentioned under Taxi Zone Maps and Lookup Tables in the NYC Taxi Data website and upload it to databricks and create a table named "taxi_zone_lookup".

Once the table is done, you are ready to query data!

**Note** There should be 48 files total from Jan 2022-Dec 2025. There are some differences in how data is recorded between those months. We've included a script you can run in the terminal (or pull out into its own python script) to combine the data. You may need to change the path variable in the line that sets files.
Once you have combined the files you can upload the single merged file to Google Cloud and Databricks.

```bash
python3 - <<'PY'                                            
import polars as pl
from pathlib import Path

files = sorted(Path("./data").glob("yellow_tripdata_20*.parquet"))

def normalize_scan(path: Path) -> pl.LazyFrame:
    lf = pl.scan_parquet(path)

    cols = set(lf.collect_schema().names())

    # normalize Airport_fee naming first
    if "Airport_fee" in cols and "airport_fee" not in cols:
        lf = lf.rename({"Airport_fee": "airport_fee"})

    cols = set(lf.collect_schema().names())

    exprs = []

    def cast_if_present(name: str, dtype):
        if name in cols:
            exprs.append(pl.col(name).cast(dtype))

    cast_if_present("VendorID", pl.Int64)
    cast_if_present("tpep_pickup_datetime", pl.Datetime("us"))
    cast_if_present("tpep_dropoff_datetime", pl.Datetime("us"))
    cast_if_present("passenger_count", pl.Float64)
    cast_if_present("trip_distance", pl.Float64)
    cast_if_present("RatecodeID", pl.Float64)
    cast_if_present("store_and_fwd_flag", pl.String)
    cast_if_present("PULocationID", pl.Int64)
    cast_if_present("DOLocationID", pl.Int64)
    cast_if_present("payment_type", pl.Int64)
    cast_if_present("fare_amount", pl.Float64)
    cast_if_present("extra", pl.Float64)
    cast_if_present("mta_tax", pl.Float64)
    cast_if_present("tip_amount", pl.Float64)
    cast_if_present("tolls_amount", pl.Float64)
    cast_if_present("improvement_surcharge", pl.Float64)
    cast_if_present("total_amount", pl.Float64)
    cast_if_present("congestion_surcharge", pl.Float64)
    cast_if_present("airport_fee", pl.Float64)

    lf = lf.with_columns(exprs)

    # make sure all expected columns exist
    expected = {
        "VendorID": pl.Int64,
        "tpep_pickup_datetime": pl.Datetime("us"),
        "tpep_dropoff_datetime": pl.Datetime("us"),
        "passenger_count": pl.Float64,
        "trip_distance": pl.Float64,
        "RatecodeID": pl.Float64,
        "store_and_fwd_flag": pl.String,
        "PULocationID": pl.Int64,
        "DOLocationID": pl.Int64,
        "payment_type": pl.Int64,
        "fare_amount": pl.Float64,
        "extra": pl.Float64,
        "mta_tax": pl.Float64,
        "tip_amount": pl.Float64,
        "tolls_amount": pl.Float64,
        "improvement_surcharge": pl.Float64,
        "total_amount": pl.Float64,
        "congestion_surcharge": pl.Float64,
        "airport_fee": pl.Float64,
    }

    cols = set(lf.collect_schema().names())
    missing_exprs = [
        pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in expected.items()
        if name not in cols
    ]
    if missing_exprs:
        lf = lf.with_columns(missing_exprs)

    return lf.select(list(expected.keys()))

dfs = [normalize_scan(f) for f in files]
pl.concat(dfs).sink_parquet("yellow_tripdata_merged.parquet")

print(f"Merged {len(files)} files into yellow_tripdata_merged.parquet")
PY
```

## Run SQL Queries and Record Performance

Go to the SQL Editor in the nav-bar on the left. This will open a window where you can write or paste SQL queries. You can access the table by using the format `WORKSPACE_NAME.default.TABLE_NAME`, i.e. `data342.default.taxi_data` as shown below. You can then run the query and see the results and the runtime as well in that same page. 

<img width="1510" alt="image" src="https://github.com/user-attachments/assets/94b3d54f-5b48-4130-aab6-24f5f830e795" />

## Setup BigQuery

We will be using Google Cloud for this class. New accounts should be able to receive $300 in credits (use your personal email id), as well as access to a free-tier of cloud usage. If you have already used these credits please contact us so we can make sure you have the resources you need to complete the assignments. 

## Loading data into BigQuery 
First you will want to upload the taxi data to Google Cloud Storage. Navigate to the Google Cloud Storage window, create a bucket with a name of your choosing (default options are fine). You can then upload the Parquet files directly to this bucket from the web UI. If you wish to do this via CLI, you can follow the guide [here](https://cloud.google.com/sdk/docs/install).  

Next, once data is uploaded, navigate to BigQuery via the left-hand navigation bar (enable the BigQuery API if it prompts you to). Once there, navigate to BigQuery studio, where it should list a project. **Make sure to note the project ID as this will be important in the next step.** 

Click the menu next to the project name, and click "create dataset". **Make sure the location type matches what's listed on your GCS bucket (i.e. multi-region, US)**. Then, we just need to load data into BigQuery to enable running SQL queries! For this part, you'll need to pick one of two options for loading data, provide some information on what you think the tradeoffs might be for each in the writeup, and follow the guides below. 

Data can be kept in Google Cloud Storage and BigQuery makes an EXTERNAL TABLE which points to this data. When you run queries against this table, it is accessing data in GCS. 

Data can also be loaded into BigQuery. This requires more time, as data must be uploaded from GCS into BigQuery, and is then stored in BQ's properietary internal format rather than the open Parquet format in GCS. 

- [External Tables](https://cloud.google.com/bigquery/docs/external-data-cloud-storage#sql)
- [Load Data Internal](https://cloud.google.com/bigquery/docs/loading-data)

After the setup, import data from January 2022 to December 2025 yellow-cab Parquet files into a single BigQuery **INTERNAL** table. 
Repeat this, but do it for an **EXTERNAL** table as well. You will run each query against both table options. Name your dataset within BigQuery **data342** to avoid having to edit the pre-written SQL workload.

## SQL Workload 
Now that we have this setup, we're going to run a SQL workload in Databricks. There are two parts to this homework, you can find the queries for each part within this repo.

In part 1, the queries have to be executed in Databricks. Make sure you are having multiple clusters in your SQL warehouse before execution. Your write-up will include the performance results and explanations for why some queries performed better in each of the three files (1,2,3). 

In part 2, SQL query files are located in two folders: `BigQuery` and `Databricks`. You're going to run each query first against the Databricks table, and then against the BigQuery table. For the first query in part 2 please report number using both the INTERNAL and EXTERNAL Tables. For queries 2-4 you may report just Databricks and internal BigQuery performance. 

You can copy and paste the queries into the editors, or read documentation online to setup APIs to execute SQL queries via APIs. Please execute the `Databricks` query on multiple clusters (1-4) and provide your analysis on the performance difference by either using EXPLAIN or looking at execution graphs. 

Run each query, record the runtime in each system, and present this in your writeup, either in a table or other format. 




