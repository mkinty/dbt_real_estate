-- Creation de la table raw
CREATE OR ALTER TABLE raw.raw_events (
    raw_payload VARIANT,
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);