--  Delete partition for load date
 DELETE FROM 
    de_zoomcamp_dataset.fct_events  t  
 WHERE 
    TIMESTAMP_TRUNC(event_ts, DAY) IN 
    (
    SELECT 
        CAST(load_dt as TIMESTAMP) AS l_ts
    FROM
    (
        SELECT    
            CASE 
                WHEN '{{ variables("p_load_date") }}' = '' 
                -- if parameter is not defined, then take day previous to execution date
                    THEN DATE_ADD(CAST(SUBSTR('{{ execution_date }}',0,10) AS DATE), INTERVAL -1 DAY)                
                ELSE CAST('{{ variables("p_load_date") }}' AS DATE)
            END
        AS load_dt
    ));



--INSERT DATA INTO PARTITION FOR LOAD DATE
INSERT INTO de_zoomcamp_dataset.fct_events 
(
  event_id,
  event_ts,
  actor_id,
  org_id,
  repo_id,
  public,
  type 
)
WITH cte_load_date as 
( -- get load date
    SELECT 
        EXTRACT(YEAR FROM load_dt) AS l_year
        ,EXTRACT(MONTH FROM load_dt) AS l_month
        ,EXTRACT(DAY FROM load_dt) AS l_day        
    FROM
    (
        SELECT    
            CASE 
                WHEN '{{ variables("p_load_date") }}' = '' 
                -- if parameter is not defined, then take day previous to execution date
                    THEN DATE_ADD(CAST(SUBSTR('{{ execution_date }}',0,10) AS DATE), INTERVAL -1 DAY)                
                ELSE CAST('{{ variables("p_load_date") }}' AS DATE)
            END
        AS load_dt
    )
)  
SELECT
 CAST(id AS NUMERIC ) AS event_id,
 CAST(created_at AS TIMESTAMP) AS event_ts,
 CAST(actor_id AS NUMERIC) AS actor_id,
 CAST(org_id AS NUMERIC) AS org_id ,
 CAST(repo_id AS NUMERIC) AS repo_id,
 CAST(public AS BOOL) AS public,
 type 
FROM
    de_zoomcamp_dataset.stage_events_external t
    CROSS JOIN cte_load_date l
WHERE 
    t.year = l.l_year
    AND t.month = l.l_month
    AND t.day = l.l_day;


