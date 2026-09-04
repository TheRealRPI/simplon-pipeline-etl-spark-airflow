# PROJET TradeCorp Data Platform
*Pipeline ETL avec Apache Spark, Docker et Azure Data Lake Storage*

## Contexte
Pipeline ETL automatisée pour nettoyer, enrichir et stocker les données commerciales de TradeCorp. Les données **CSV et JSON** brutes (incluant les fichiers de référence pour l'enrichissement) sont téléchargées depuis **Azure Blob Storage**, transformées avec **PySpark** (nettoyage, déduplication, calculs, conversions de devises), puis stockées au format **Parquet** dans **ADLS**.

## Architecture
```
Azure Blob Storage (raw: CSV/JSON + reference/) → Pipeline (Nettoyage + Enrichissement + Conversions devises) → ADLS (clean: Parquet)
```

## Stack Technique
- **ETL**: Apache Spark (PySpark) avec Hadoop Azure 3.3.4
- **Orchestration**: Docker Compose
- **Stockage**: Azure Data Lake Storage Gen2
- **Base de données**: PostgreSQL 16 + pgAdmin
- **Langage**: Python 3.x
- **Outils**: Jupyter Notebooks, azure-storage-blob, python-dotenv, requests
- **Tests**: pytest

## Structure du Projet
```
/SPARK
├── src/                  # Code source (pipeline, reader, transformer, writer, utils, enrichment, fetch_exchange_rates)
├── data/                 # Données locales et fichiers de référence
│   └── reference/        # Fichiers de référence (country_currency.csv, exchange_rates.json)
├── notebooks/            # Notebooks Jupyter pour exploration et tests
├── tests/                # Tests unitaires (pytest)
│   ├── run_tests.py
│   └── test_transformers.py
├── docker/               # Configuration Docker
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env
```

## Fonctionnalités

### Pipeline ETL
- **Pipeline**: Script principal (`pipeline.py`) orchestrant le flux ETL complet avec logging détaillé (4 étapes : téléchargement → DataFrames → enrichissement → écriture)
- **Reader**: Téléchargement des **CSV et JSON** depuis ADLS (incluant les fichiers dans les sous-dossiers comme `reference/`) et création de DataFrames Spark
- **Transformer**: Nettoyage (trim, déduplication, cast, suppression des lignes nulles), enrichissement (jointures, calculs comme `sous_total`)
- **Writer**: Écriture des données nettoyées vers ADLS au format **Parquet**
- **Utils**: Fonctions utilitaires pour Azure (connexion, upload/download), Spark (session, types) et transformations génériques (trim, renommage, cast)

### Enrichissement des Données
- **Conversions de devises**: Ajout de la colonne `sous_total_local` via jointure avec les fichiers de référence
- **Fetch des taux de change**: Récupération automatique des taux depuis **ExchangeRate-API** et upload vers ADLS

### Tests Unitaires
- **pytest** : Tests complets pour `clean_orders`, `add_sous_total`, `clean_customers`, `add_currency_column`

## Prérequis
- Docker et Docker Compose
- Compte Azure avec ADLS Gen2
- Fichier `.env` avec les credentials Azure :
  ```ini
  AZURE_STORAGE=<nom-du-storage>
  AZURE_KEY=<clé-d'accès>
  ```
- Accès internet pour le fetch des taux de change (API ExchangeRate-API)

## Installation
1. Cloner le projet et accéder au dossier
2. Créer `.env` avec `AZURE_STORAGE` et `AZURE_KEY`
3. Lancer les services : `docker-compose up -d --build`

Les services seront accessibles sur :
- Jupyter Notebook: http://localhost:8888
- pgAdmin: http://localhost:8080
- PostgreSQL: http://localhost:5434

## Utilisation

### Exécuter le pipeline complet
```bash
docker exec tradecorp_spark spark-submit /home/jovyan/src/pipeline.py
```

### Exécuter les tests unitaires
```bash
docker exec tradecorp_spark python /home/jovyan/tests/run_tests.py
```

### Mettre à jour les taux de change
```bash
docker exec tradecorp_spark python /home/jovyan/src/fetch_exchange_rates.py
```

## Configuration Azure

### Stockage ADLS
- **Conteneur `raw`** :
  - Fichiers CSV bruts (`categories.csv`, `customers.csv`, `orders.csv`, etc.)
  - Sous-dossier **`reference/`** contenant :
    - `country_currency.csv` (mapping pays → devise)
    - `exchange_rates.json` (taux de change USD → toutes devises)
- **Conteneur `clean`** :
  - Fichiers Parquet enrichis (ex: `orders_enriched`)

### Credentials
- Stockés dans `.env` (à la racine du projet)

## Roadmap
- [x] Pipeline ETL complète (Reader, Transformer, Writer)
- [x] Environnement Docker avec Jupyter et PostgreSQL
- [x] Support des fichiers JSON en entrée
- [x] Gestion des sous-dossiers dans le reader
- [x] Enrichissement des données avec conversions de devises
- [x] Tests unitaires complets avec pytest
- [ ] Intégration Airflow
- [ ] Déploiement production
- [ ] Monitoring et alerting

## Auteurs
TheRealRPI - Data Engineer Consultant pour TradeCorp International

## License
Projet académique - Simplon.co
