# PROJET TradeCorp Data Platform
*Pipeline ETL avec Apache Spark, Docker et Azure Data Lake Storage*

## Contexte
Pipeline ETL automatisée pour nettoyer, enrichir et stocker les données commerciales de TradeCorp. Les données CSV brutes sont téléchargées depuis Azure Blob Storage, transformées avec PySpark, puis stockées au format Parquet dans ADLS.

## Architecture
```
Azure Blob Storage (raw) → Pipeline → ADLS (clean)
```

## Stack Technique
- **ETL**: Apache Spark (PySpark)
- **Orchestration**: Docker Compose
- **Stockage**: Azure Data Lake Storage Gen2
- **Base de données**: PostgreSQL 16 + pgAdmin
- **Langage**: Python 3.x
- **Outils**: Jupyter Notebooks, azure-storage-blob, python-dotenv

## Structure du Projet
```
/SPARK
├── src/                  # Code source (pipeline, reader, transformer, writer, utils)
├── notebooks/            # Notebooks Jupyter pour exploration et tests
├── data/                 # Données locales
├── tests/                # Tests unitaires
├── docker/               # Configuration Docker
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env
```

## Fonctionnalités
- **Pipeline**: Script principal orchestrant le flux ETL complet
- **Reader**: Téléchargement des CSV depuis ADLS et création de DataFrames Spark
- **Transformer**: Nettoyage (trim, déduplication, cast), enrichissement (jointures, calculs)
- **Writer**: Écriture des données nettoyées vers ADLS au format Parquet
- **Utils**: Fonctions utilitaires pour Azure, Spark et transformations génériques

## Prérequis
- Docker et Docker Compose
- Compte Azure avec ADLS Gen2
- Fichier `.env` avec les credentials Azure

## Installation
1. Cloner le projet et accéder au dossier
2. Créer `.env` avec `AZURE_STORAGE` et `AZURE_KEY`
3. Lancer les services : `docker-compose up -d`

Les services seront accessibles sur :
- Jupyter Notebook: http://localhost:8888
- pgAdmin: http://localhost:8080
- PostgreSQL: localhost:5434

## Utilisation
Exécuter le pipeline complet :
```bash
docker exec tradecorp_spark spark-submit /home/jovyan/src/pipeline.py.
```

## Configuration Azure
- Conteneurs ADLS: `raw` (CSV bruts), `clean` (Parquet nettoyé)
- Credentials stockés dans `.env`

## Roadmap
- [x] Pipeline ETL complète (Reader, Transformer, Writer)
- [x] Environnement Docker avec Jupyter et PostgreSQL
- [ ] Intégration Airflow
- [ ] Déploiement production
- [ ] Monitoring et tests unitaires

## Auteurs
TheRealRPI - Data Engineer Consultant pour TradeCorp International

## License
Projet académique - Simplon.co
