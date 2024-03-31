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
select * from cte_load_date-- Docs: https://docs.mage.ai/guides/sql-blocks
