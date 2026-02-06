# Real Estate Investment Data Pipeline (dbt + Snowflake)

## Objectif

Ce projet a pour objectif de transformer des données JSON issues d’un système de streaming événementiel d’une entreprise d’investissement immobilier en tables relationnelles historisées selon le modèle **SCD Type 2.**

Les données représentent des *snapshots complets* de l’état d’un projet immobilier à un instant donné.

Les tables cibles sont :

* **projects** : informations métier des projets immobiliers
* **address** : adresses associées aux projets
* **investors** : participations des investisseurs dans chaque projet et entité légale

---

## Architecture du pipeline

Le pipeline est structuré selon les bonnes pratiques analytics engineering : **RAW → STAGING → CURATION**.

### Flux global

1. Ingestion des fichiers JSON dans Snowflake (RAW)
2. Normalisation et nettoyage avec dbt (STAGING)
3. Historisation SCD2 via un code sql (CURATION)

---

## Lineage du projet

Le graphique ci-dessous illustre le lineage complet des données depuis la table RAW jusqu’aux tables finales de curation.

Il permet de visualiser :

* les dépendances entre les modèles dbt
* le passage par du code SQL pour organiser les données au format SCD2
* les relations entre projets, adresses et investisseurs

![Data Lineage](docs/lineage.png)

> Le lineage a été généré via **dbt Docs** afin de garantir une vision exhaustive et maintenable du pipeline.

---

## 1. Ingestion RAW

Les données arrivent sous forme de fichiers JSON (1 événement = 1 snapshot complet d’un projet).

### Table RAW

```sql
CREATE OR ALTER TABLE raw.raw_events (
    raw_payload VARIANT,
    load_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);
```

### Ingestion automatique

* Stage interne Snowflake
* Pipe avec `AUTO_INGEST = TRUE`
* Chargement du JSON complet dans `raw_payload`

Cette couche est **immutables** : aucune transformation métier n’y est appliquée.

---

## 2. Staging (normalisation)

Schéma : `staging`

Objectifs du staging :

* Extraire les champs JSON
* Aplatir les structures imbriquées (`investors`)

### Modèles de staging

* `stg_projects`
* `stg_address`
* `stg_investors`



### Modèles curation (historisées pour organiser les données aux format SCD2)

* `scd_projects`
* `scd_address`
* `scd_investors`

---

## Tests de qualité

Les tests dbt garantissent la fiabilité du pipeline.

### Types de tests

* `not_null`
* `unique`
* `relationships`

### Exemple

* `project_identifier` doit exister dans `stg_projects`
* `address.project_identifier` référence un projet valide

---

## Cas métier gérés

* Les données sont des snapshots complets (pas des deltas)
* Les investisseurs peuvent :

  * augmenter / diminuer leur investissement
  * sortir complètement d’un projet
* La sortie d’un investisseur est détectée via :

  * `invested = 0`

---

## Commandes dbt utiles

```bash
dbt deps
# Installation des packages

dbt build
# Build des modèles + snapshots

dbt test
# Tests de qualité

dbt docs generate && dbt docs serve
# Documentation et lineage
```

---

## Technologies

* **Snowflake** – Data warehouse
* **dbt Core** – Transformations & SCD2
* **SQL** – Modélisation
* **GitHub** – Versioning et partage

---

Projet réalisé dans le cadre d’un exercice d’analytics engineering visant à démontrer :

* la modélisation SCD2
* la gestion de données événementielles
* les bonnes pratiques dbt & Snowflake

---

Toute suggestion ou revue est la bienvenue.
