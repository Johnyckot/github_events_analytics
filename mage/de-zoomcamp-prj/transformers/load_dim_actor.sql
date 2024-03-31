

-- --upsert actor dimension with staging data (for a given day)

MERGE INTO de_zoomcamp_dataset.dim_actor tgt
USING
(
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
        t.id AS actor_id 
        ,t.login AS actor_login
        ,t.url AS actor_url 
    FROM de_zoomcamp_dataset.stage_actor_external t
        CROSS JOIN cte_load_date l
    WHERE 
        t.year = l.l_year
        AND t.month = l.l_month
        AND t.day = l.l_day
) src
ON tgt.actor_id = src.actor_id 
WHEN MATCHED 
  AND( tgt.actor_login != src.actor_login 
   OR tgt.actor_url != src.actor_url)
THEN UPDATE
  SET 
  tgt.actor_login = src.actor_login,
  tgt.actor_url = src.actor_url
WHEN NOT MATCHED THEN 
 INSERT (actor_id, actor_login, actor_url)
 VALUES (src.actor_id, src.actor_login, src.actor_url);  -- Docs: https://docs.mage.ai/guides/sql-blocks
