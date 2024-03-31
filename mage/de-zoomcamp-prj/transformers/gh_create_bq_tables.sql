--  CREATES TABLES IN  BIGQUERY IF THEY DON'T EXIST YET

--  CREATE External tables based on Stage Parquet files. Tables are partitioned by year/month/day, according to the folder structure in GCS

CREATE EXTERNAL TABLE IF NOT EXISTS de_zoomcamp_dataset.stage_repo_external
WITH PARTITION COLUMNS (
year INT64, 
month INT64,
day INT64)
OPTIONS (
uris = ['gs://{{ variables("p_gcs_bucket_name") }}/{{ variables("p_gcs_stage_path") }}repo/*'],
format = 'PARQUET',
hive_partition_uri_prefix = 'gs://{{ variables("p_gcs_bucket_name") }}/{{ variables("p_gcs_stage_path") }}repo/',
require_hive_partition_filter = false);


CREATE EXTERNAL TABLE IF NOT EXISTS de_zoomcamp_dataset.stage_org_external
WITH PARTITION COLUMNS (
year INT64, 
month INT64,
day INT64)
OPTIONS (
uris = ['gs://{{ variables("p_gcs_bucket_name") }}/{{ variables("p_gcs_stage_path") }}org/*'],
format = 'PARQUET',
hive_partition_uri_prefix = 'gs://{{ variables("p_gcs_bucket_name") }}/{{ variables("p_gcs_stage_path") }}org/',
require_hive_partition_filter = false);


CREATE EXTERNAL TABLE IF NOT EXISTS de_zoomcamp_dataset.stage_actor_external
WITH PARTITION COLUMNS (
year INT64, 
month INT64,
day INT64)
OPTIONS (
uris = ['gs://{{ variables("p_gcs_bucket_name") }}/{{ variables("p_gcs_stage_path") }}actor/*'],
format = 'PARQUET',
hive_partition_uri_prefix = 'gs://{{ variables("p_gcs_bucket_name") }}/{{ variables("p_gcs_stage_path") }}actor/',
require_hive_partition_filter = false);

CREATE EXTERNAL TABLE IF NOT EXISTS de_zoomcamp_dataset.stage_events_external
WITH PARTITION COLUMNS (
year INT64, 
month INT64,
day INT64)
OPTIONS (
uris = ['gs://{{ variables("p_gcs_bucket_name") }}/{{ variables("p_gcs_stage_path") }}events/*'],
format = 'PARQUET',
hive_partition_uri_prefix = 'gs://{{ variables("p_gcs_bucket_name") }}/{{ variables("p_gcs_stage_path") }}events/',
require_hive_partition_filter = false);




-- CREATE DIMENSIONS (BQ Standart tables)

-- DROP TABLE IF EXISTS de_zoomcamp_dataset.dim_repo ;
CREATE TABLE IF NOT EXISTS de_zoomcamp_dataset.dim_repo (
  repo_id NUMERIC(20,0) OPTIONS (description = 'Repository ID'),
  repo_name  STRING OPTIONS (description = 'Repository name'),
  repo_url  STRING OPTIONS (description = 'Repository URL')  
) 
CLUSTER BY
  repo_id
OPTIONS (    
    description = 'Repository dimension'
)
;


-- DROP TABLE IF EXISTS de_zoomcamp_dataset.dim_actor ;
CREATE TABLE IF NOT EXISTS de_zoomcamp_dataset.dim_actor  (
  actor_id NUMERIC(20,0) OPTIONS (description = 'Actor ID'),
  actor_login  STRING OPTIONS (description = 'Actor login'),
  actor_url  STRING OPTIONS (description = 'Actor URL')  
) 
CLUSTER BY
  actor_id
OPTIONS (    
    description = 'Actor dimension'
)
;

-- DROP TABLE IF EXISTS de_zoomcamp_dataset.dim_org ;
CREATE TABLE IF NOT EXISTS de_zoomcamp_dataset.dim_org  (
  org_id NUMERIC(20,0) OPTIONS (description = 'Organization ID'),
  org_login  STRING OPTIONS (description = 'Organization login'),
  org_url  STRING OPTIONS (description = 'Organization URL')  
) 
CLUSTER BY
  org_id
OPTIONS (    
    description = 'Organization dimension'
)
;


-- CREATE FACT TABLES (BQ Standart tables)

-- Daily aggregated events fact table (joined with dimensions)
-- DROP TABLE IF EXISTS de_zoomcamp_dataset.fct_events_dd ;
CREATE TABLE IF NOT EXISTS de_zoomcamp_dataset.fct_events_dd  (  
  event_date  DATE OPTIONS (description = 'Event date'),
  actor_login STRING OPTIONS (description = 'actor url'),
  actor_url STRING OPTIONS (description = 'actor url'),
  org_login STRING OPTIONS (description = 'Org login'),
  org_url STRING OPTIONS (description = 'Org url'),
  repo_name STRING OPTIONS (description = 'Repo name'),
  repo_url STRING OPTIONS (description = 'Repo url'),
  public BOOL OPTIONS (description = 'Public flag'),
  event_type STRING OPTIONS (description = 'Event type'),
  count NUMERIC(20,0) OPTIONS (description = 'Count events')
)
PARTITION BY
  event_date  
OPTIONS (    
    description = 'Events fact table aggregated on daily level'
)
;


-- Granular events fact table
-- DROP TABLE IF EXISTS de_zoomcamp_dataset.fct_events
CREATE TABLE IF NOT EXISTS de_zoomcamp_dataset.fct_events  (
  event_id NUMERIC(20,0) OPTIONS (description = 'Event ID'),
  event_ts  TIMESTAMP OPTIONS (description = 'Event timestamp'),
  actor_id NUMERIC(20,0) OPTIONS (description = 'Actor ID'),
  org_id NUMERIC(20,0) OPTIONS (description = 'Org ID'),
  repo_id NUMERIC(20,0) OPTIONS (description = 'Repo ID'),
  public BOOL OPTIONS (description = 'Public flag'),
  type STRING OPTIONS (description = 'Event type')
)
PARTITION BY
  TIMESTAMP_TRUNC(event_ts, DAY) 
CLUSTER BY
  event_id
OPTIONS (    
    description = 'Events fact table'
)
;

