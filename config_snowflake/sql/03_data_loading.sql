-- Utiliser la database realestate
USE DATABASE REALESTATE;

-- Creation du file format json
CREATE OR REPLACE FILE FORMAT raw.json_object_format
TYPE = 'JSON';

-- Création du stage interne
CREATE OR REPLACE STAGE raw.raw_json_stage
FILE_FORMAT = raw.json_object_format;


-- Création d'un pipe d'ingestion automatique
CREATE OR REPLACE PIPE raw.load_raw_data
  AUTO_INGEST = TRUE
  AS
    COPY INTO raw.raw_events (raw_payload)
    FROM (
        SELECT $1 
        FROM @raw.raw_json_stage)
  FILE_FORMAT = raw.json_object_format;




