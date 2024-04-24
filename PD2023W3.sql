--Written in Snowflake
--CTE to unpivot Targets
WITH TablePivot AS (
                    SELECT *
                    FROM    PD2023_WK03_TARGETS as t
                    UNPIVOT(quarterly_targets FOR quarterunclean IN ("Q1", "Q2","Q3","Q4") )
                    )
--CTE to get value per channel and quarter
, Transaction AS (
        --Get text from codes
        SELECT REGEXP_SUBSTR(w12023.transaction_code,'[A-Z]+') AS BANK
        ,SUM(w12023.value) AS VALUE
        ,
        -- Convert string in form dd/MM/yyyy hh/mm/ss to quarter
        QUARTER(
            to_date(
                left(
                    w12023.transaction_date
                    ,10
                    )
                ,'DD/MM/YYYY') 
            ) AS quarter
        ,CASE TO_CHAR(w12023.online_or_in_person)
        WHEN '1' THEN 'Online'
        WHEN '2' THEN 'In-Person'
        END AS online_or_in_person
        
    FROM PD2023_WK01 AS w12023
    WHERE BANK = 'DSB'
    GROUP BY quarter, online_or_in_person, BANK
                    )
        
SELECT  
        tr.QUARTER
        ,tr.ONLINE_OR_IN_PERSON
        ,tr.VALUE
        ,ta.QUARTERLY_TARGETS
        ,tr.VALUE-ta.QUARTERLY_TARGETS AS variance_to_target
FROM TRANSACTION as tr
INNER JOIN (Select *
                    --clean quarter field before join
                    ,RIGHT(t.quarterunclean,1) AS quarter
                FROM TablePivot AS t) as ta
on tr.QUARTER = ta.QUARTER and tr.online_or_in_person = ta.online_or_in_person


;
