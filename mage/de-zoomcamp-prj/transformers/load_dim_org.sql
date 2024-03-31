

-- --upsert org dimension with staging data (for a given day)

MERGE INTO de_zoomcamp_dataset.dim_org tgt
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
        t.id AS org_id 
        ,t.login AS org_login
        ,t.url AS org_url 
    FROM de_zoomcamp_dataset.stage_org_external t
        CROSS JOIN cte_load_date l
    WHERE 
        t.year = l.l_year
        AND t.month = l.l_month
        AND t.day = l.l_day
) src
ON tgt.org_id = src.org_id 
WHEN MATCHED 
  AND( tgt.org_login != src.org_login 
   OR tgt.org_url != src.org_url)
THEN UPDATE
  SET 
  tgt.org_login = src.org_login,
  tgt.org_url = src.org_url
WHEN NOT MATCHED THEN 
 INSERT (org_id, org_login, org_url)
 VALUES (src.org_id, src.org_login, src.org_url);  