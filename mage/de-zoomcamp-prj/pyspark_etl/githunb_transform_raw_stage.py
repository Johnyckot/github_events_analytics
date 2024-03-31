import argparse
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('Transofrm_github_data') \
    .getOrCreate()


parser = argparse.ArgumentParser()

parser.add_argument('--input_path', required=True)
parser.add_argument('--output_path', required=True)
parser.add_argument('--load_date', required=True)

args = parser.parse_args()

input_path = args.input_path
output_path = args.output_path
load_date = args.load_date


deptDF = spark.read.json(input_path)

deptDF.cache()
deptDF.createOrReplaceTempView('v_github_events')


# dataframe with Repositories info. Only one entry for each id is taken (latest)
df_repo = spark.sql("""
    SELECT 
          id
          ,name
          ,url 
    FROM
        (SELECT 
            repo.id
            ,repo.name
            ,repo.url
            ,row_number() over(partition by repo.id order by created_at desc) as rn
        FROM 
            v_github_events) 
    WHERE rn = 1
""")

# dataframe with Organizations info. Only one entry for each id is taken (latest)
df_org = spark.sql("""
    SELECT 
          id
          ,login
          ,url 
    FROM
        (SELECT 
            org.id
            ,org.login
            ,org.url
            ,row_number() over(partition by org.id order by created_at desc) as rn
        FROM 
            v_github_events) 
    WHERE rn = 1
""")

# dataframe with Actors info. Only one entry for each id is taken (latest)
df_actor = spark.sql("""
    SELECT 
          id
          ,login
          ,url 
    FROM
        (SELECT 
            actor.id
            ,actor.login
            ,actor.url
            ,row_number() over(partition by actor.id order by created_at desc) as rn
        FROM 
            v_github_events) 
    WHERE rn = 1
""")

# dataframe with Events info
df_events = spark.sql("""
    SELECT 
        created_at
        ,id
        ,actor.id as actor_id
        ,org.id as org_id
        ,repo.id as repo_id
        ,public
        ,type
    FROM 
        v_github_events;
""")


partition_path = f"year={load_date.split('-')[0]}/month={load_date.split('-')[1].lstrip('0')}/day={load_date.split('-')[2].lstrip('0')}"


# Save data to parqute file
df_repo\
    .coalesce(1)\
    .write\
    .mode('overwrite')\
    .parquet(f'{output_path}/repo/{partition_path}')

df_actor\
    .coalesce(1)\
    .write\
    .mode('overwrite')\
    .parquet(f'{output_path}/actor/{partition_path}')

df_org\
    .coalesce(1)\
    .write\
    .mode('overwrite')\
    .parquet(f'{output_path}/org/{partition_path}')

df_events\
    .coalesce(2)\
    .write.mode('overwrite')\
    .parquet(f'{output_path}/events/{partition_path}')
