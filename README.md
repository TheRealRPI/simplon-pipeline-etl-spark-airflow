# PROJET TradeCorp Data Platform
*Pipeline ETL industrialisé avec Apache Spark, Docker et Azure Data Lake Storage*

## Contexte
TradeCorp International reçoit chaque nuit ses données commerciales sous forme de fichiers CSV bruts. Ce projet implémente une **pipeline ETL automatisée** pour :
- Télécharger les données depuis Azure Blob Storage (zone raw)
- Nettoyer et transformer les données avec PySpark
- Enrichir les données via des jointures entre tables
- Stocker les résultats dans Azure Data Lake Storage (zone clean) au format Parquet
- Proposer un environnement de développement conteneurisé avec Docker

## Architecture

```
Azure Blob Storage (raw)
    └── CSV Files: categories, customers, employees, order_details, orders, products, shippers, suppliers, country_currency
        
Reader (reader.py)
    └── Téléchargement des fichiers → ./data/
        
Transformer (transformer.py)
    └── Nettoyage: trim, déduplication, cast, rename
    └── Enrichissement: jointures entre tables
    └── Calculs: sous_total, is_shipped, en_stock, full_name
        
Writer (writer.py)
    └── Écriture vers ADLS (clean) au format Parquet
        
Azure Data Lake Storage (clean)
    └── Tables enrichies: orders_enriched
```

### Stack Technique

| Composant | Technologie | Version |
|-----------|-------------|---------|
| **Orchestration** | Docker Compose | - |
| **Notebooks** | Jupyter PySpark | latest |
| **ETL** | Apache Spark (PySpark) | 3.x |
| **Stockage Cloud** | Azure Data Lake Storage Gen2 | - |
| **Base de données** | PostgreSQL | 16 |
| **Admin DB** | pgAdmin 4 | latest |
| **Langage** | Python | 3.x |
| **Gestion des secrets** | python-dotenv | - |
| **SDK Azure** | azure-storage-blob | - |

## Structure du Projet

```
📁 /SPARK
├── 📁 src/
│   ├── reader.py          # Téléchargement des fichiers CSV depuis ADLS
│   ├── transformer.py     # Nettoyage et enrichissement des données
│   ├── writer.py          # Écriture des DataFrames vers ADLS
│   └── utils.py           # Fonctions utilitaires (Azure, Spark, transformations)
│
├── 📁 notebooks/
│   ├── 01_exploration.ipynb     # Exploration des données
│   ├── 02_nettoyage.ipynb       # Tests de nettoyage
│   ├── 03_transformations.ipynb  # Tests de transformations
│   └── ADLS_Connect.ipynb        # Connexion et tests ADLS
│
├── 📁 data/               # Données locales (monté dans le container)
├── 📁 tests/              # Tests unitaires
├── 📁 docker/
│
├── Dockerfile             # Image Jupyter PySpark avec dépendances
├── docker-compose.yml     # Services: Jupyter, PostgreSQL, pgAdmin
├── requirements.txt       # Dépendances Python
├── .env                   # Variables d'environnement (Azure credentials)
└── README.md
```

## Fonctionnalités Implémentées

### Reader (reader.py)
- `download_blobs_from_list()`: Télécharge les fichiers CSV depuis ADLS vers `./data/`
- `create_dataframes_from_files()`: Crée des DataFrames Spark à partir des fichiers locaux
- Fichiers gérés: categories, country_currency, customers, employees, order_details, orders, products, shippers, suppliers

### Transformer (transformer.py)
**Fonctions de nettoyage par table:**
- `clean_customers()`: trim, initcap, upper, déduplication, renommage des colonnes
- `clean_orders()`: suppression des nulls, cast des dates, ajout de `is_shipped`
- `clean_order_details()`: cast, renommage (prix_unitaire, quantite, remise), calcul de `sous_total`
- `clean_employees()`: sélection des colonnes, création de `full_name`, renommage
- `clean_products()`: cast, jointure avec categories, ajout de `en_stock`
- `clean_shipper()`: renommage des colonnes, camel case

**Fonction d'enrichissement:**
- `build_enriched()`: Jointure de toutes les tables nettoyées en un DataFrame unique avec 22 colonnes

### Writer (writer.py)
- `write_df_to_adls()`: Écriture des DataFrames vers ADLS au format Parquet (mode overwrite)

### Utils (utils.py)
**Connexion Azure:**
- `get_azure_info()`: Récupération des credentials depuis .env
- `get_blob_service_client()`: Création du client BlobServiceClient
- `download_blob_from_file()`: Téléchargement d'un blob spécifique

**Session Spark:**
- `get_spark_session()`: Création d'une session Spark configurée pour ADLS

**Fonctions de transformation génériques:**
- `trim_col()`, `camel_trim_col()`, `upper_trim_col()`: Nettoyage de colonnes
- `deduplicate()`: Suppression des doublons
- `delete_col_if_null()`: Suppression des lignes avec null
- `cast_to_type()`: Cast de colonnes (date, double, integer)
- `rename_col()`, `rename_cols()`: Renommage de colonnes
- `add_col_with_condition()`: Ajout de colonne conditionnelle

## Prérequis

- Docker et Docker Compose
- Compte Azure avec ADLS Gen2 configuré
- Fichiers .env avec les credentials Azure

## Installation et Exécution

### 1. Cloner le projet
```bash
git clone https://github.com/TheRealRPI/simplon-pipeline-etl-spark-airflow.git
cd simplon-pipeline-etl-spark-airflow
```

### 2. Configurer les variables d'environnement
Créer un fichier `.env` à la racine du projet :
```
AZURE_STORAGE=<votre_storage_account>
AZURE_KEY=<votre_access_key>
```

### 3. Lancer les services avec Docker Compose
```bash
docker-compose up -d
```

Les services suivants seront disponibles :
- **Jupyter Notebook**: http://localhost:8888 (token dans les logs)
- **PostgreSQL**: localhost:5434 (user: tradecorp, password: tradecorp, db: tradecorp)
- **pgAdmin**: http://localhost:8080 (email: admin@tradecorp.com, password: admin)

### 4. Exécuter la pipeline

Dans un notebook Jupyter ou un script Python :

```python
from reader import create_dataframes_from_files
from transformer import build_enriched
from writer import write_df_to_adls
from utils import get_azure_info

# Charger les données
df_dict = create_dataframes_from_files()

# Nettoyer et enrichir
df_enriched = build_enriched(df_dict)

# Écrire vers ADLS
storage, _ = get_azure_info()
write_df_to_adls(df_enriched, "orders_enriched", "clean", storage)
```

## Configuration Azure

La pipeline utilise ADLS Gen2 avec les conteneurs suivants :
- `raw`: Contient les fichiers CSV bruts
- `clean`: Stocke les données nettoyées au format Parquet

La connexion est sécurisée via :
- Stockage des credentials dans `.env` (exclus du dépôt via `.gitignore`)
- Configuration automatique de Spark pour ADLS

## Dépendances

Voir `requirements.txt` :
```
python-dotenv
azure-storage-blob
requests
pytest
apache-airflow-providers-docker
```

## Roadmap

- [x] Implémentation du reader pour téléchargement depuis ADLS
- [x] Fonctions de nettoyage pour toutes les tables
- [x] Fonctions d'enrichissement et jointures
- [x] Writer pour ADLS en format Parquet
- [x] Configuration Docker avec Jupyter, PostgreSQL, pgAdmin
- [x] Notebooks d'exploration et de test
- [ ] Intégration avec Apache Airflow pour l'orchestration
- [ ] Ajout des DAGs Airflow pour l'exécution automatique
- [ ] Configuration Azure Key Vault pour la gestion des secrets
- [ ] Déploiement en production
- [ ] Monitoring et logging
- [ ] Tests unitaires complets

## Auteurs

TheRealRPI - Data Engineer Consultant pour TradeCorp International

## License

Projet académique - Simplon.co
