
--upsert repo dimension with staging data (for a given day)

MERGE INTO de_zoomcamp_dataset.dim_repo tgt
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
        t.id AS repo_id 
        ,t.name AS repo_name
        ,t.url AS repo_url 
    FROM de_zoomcamp_dataset.stage_repo_external t
        CROSS JOIN cte_load_date l
    WHERE 
        t.year = l.l_year
        AND t.month = l.l_month
        AND t.day = l.l_day
) src
ON tgt.repo_id = src.repo_id 
WHEN MATCHED 
  AND( tgt.repo_name != src.repo_name 
   OR tgt.repo_url != src.repo_url)
THEN UPDATE
  SET 
  tgt.repo_name = src.repo_name,
  tgt.repo_url = src.repo_url
WHEN NOT MATCHED THEN 
 INSERT (repo_id, repo_name, repo_url)
 VALUES (src.repo_id, src.repo_name, src.repo_url);