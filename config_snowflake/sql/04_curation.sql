-- Utiliser la database realestate
USE DATABASE REALESTATE;



-- tables dans le schema raw
SELECT * FROM raw.raw_events;


-- tables dans le schema staging
SELECT * 
FROM staging.stg_projects;

SELECT * 
FROM staging.stg_address 
order by event_timestamp;

SELECT * 
FROM staging.stg_investors;



-- tables dans le schema curation
SELECT * 
FROM curation.scd_projects;

SELECT * 
FROM curation.scd_address;

SELECT * 
FROM curation.scd_investors; 


