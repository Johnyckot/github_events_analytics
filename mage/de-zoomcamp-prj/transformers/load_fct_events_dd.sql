 --DELETE DATA FROM PARTITION
 DELETE FROM 
    de_zoomcamp_dataset.fct_events_dd t  
 WHERE 
    event_date IN  (
        SELECT    
            CASE 
                WHEN '{{ variables("p_load_date") }}' = '' 
                -- if parameter is not defined, then take day previous to execution date
                    THEN DATE_ADD(CAST(SUBSTR('{{ execution_date }}',0,10) AS DATE), INTERVAL -1 DAY)                
                ELSE CAST('{{ variables("p_load_date") }}' AS DATE)
            END
        AS load_dt 
    );


--INSERT DATA INTO PARTITION FOR LOAD DATE
INSERT INTO  de_zoomcamp_dataset.fct_events_dd
(
 event_date,
 actor_login,
 actor_url, 
 org_login,
 org_url,
 repo_name,
 repo_url,
 event_type,
 public,
 count
)
WITH cte_load_date as 
( -- get load date
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
    )
)   
SELECT 
 CAST(TIMESTAMP_TRUNC(f.event_ts, DAY) AS DATE) as event_date,
 a.actor_login,
 a.actor_url, 
 o.org_login,
 o.org_url,
 r.repo_name,
 r.repo_url,
 type as event_type,
 public,
 COUNT(*) as count
FROM 
  de_zoomcamp_dataset.fct_events f 
  LEFT JOIN de_zoomcamp_dataset.dim_actor a on f.actor_id = a.actor_id
  LEFT JOIN de_zoomcamp_dataset.dim_org o on f.org_id = o.org_id
  LEFT JOIN de_zoomcamp_dataset.dim_repo r on f.repo_id = r.repo_id  
WHERE 
  TIMESTAMP_TRUNC(f.event_ts, DAY) IN (SELECT l_ts FROM cte_load_date) 
GROUP BY CAST(TIMESTAMP_TRUNC(f.event_ts, DAY) AS DATE),
 a.actor_login,
 a.actor_url, 
 o.org_login,
 o.org_url,
 r.repo_name,
 r.repo_url,
 type,
 public
;