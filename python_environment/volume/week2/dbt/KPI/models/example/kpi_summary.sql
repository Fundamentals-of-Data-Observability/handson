
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT 'yellow' AS Taxi_Color, * FROM {{ref('kpi_yellow')}} UNION ALL
SELECT 'green' AS Taxi_Color, * FROM {{ref('kpi_green')}} 
