
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT DISTINCT passenger_count,
    Count(*) as CountTrips
    , ROUND(SUM(fare_amount)) as TotalFares
    , ROUND(AVG(fare_amount)) as AvgFares
FROM     `{{ env_var('GOOGLE_CLOUD_PROJECT') }}.dataobservability.green_ingestion`
GROUP BY passenger_count
ORDER BY  AvgFares DESC