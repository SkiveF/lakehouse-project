# Sécurité - Variables d'Environnement

Ce document explique comment utiliser les variables d'environnement pour les credentials au lieu de les hardcoder.

## Corrections Apportées

### 1. **streaming/consumer.py**
- Avant: Credentials en clair avec valeurs par défaut `admin` et `password123`
- Après: Obligatoirement lire depuis variables d'environnement, sans valeurs par défaut

### 2. **lakehouse_dbt/profiles.yml**
- Avant: Host et port en dur
- Après: Utilise les variables d'environnement avec variables de template

### 3. **dags/lakehouse_pipeline.py**
- Avant: JDBC connection string en dur à `localhost:10000`
- Après: Construit dynamiquement depuis les variables d'environnement

## Variables d'Environnement Requises

### Fichier `.env` (créer en racine du projet)

```dotenv
# MinIO Configuration
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=changeme_secure_password

# Spark S3A Configuration (pour accès MinIO)
SPARK_S3A_ACCESS_KEY=admin
SPARK_S3A_SECRET_KEY=changeme_secure_password

# Airflow Configuration
AIRFLOW_ADMIN_PASSWORD=changeme_airflow_password

# dbt Spark Configuration (optionnel)
DBT_SPARK_HOST=spark-thrift
DBT_SPARK_PORT=10000
DBT_SPARK_USER=root
```

## Utilisation avec Docker Compose

### Option 1: Fichier `.env` (recommandé)

1. Copier `.env.example` vers `.env`
2. Modifier les valeurs de passwords dans `.env`
3. Lancer Docker Compose:

```bash
cd docker
docker-compose --env-file ../.env up -d
```

### Option 2: Variables d'Environnement Shell (Windows PowerShell)

```powershell
# Définir les variables
$env:MINIO_ROOT_USER = "admin"
$env:MINIO_ROOT_PASSWORD = "your_secure_password"
$env:SPARK_S3A_ACCESS_KEY = "admin"
$env:SPARK_S3A_SECRET_KEY = "your_secure_password"
$env:AIRFLOW_ADMIN_PASSWORD = "your_airflow_password"
$env:DBT_SPARK_HOST = "spark-thrift"
$env:DBT_SPARK_PORT = "10000"
$env:DBT_SPARK_USER = "root"

# Lancer docker-compose
cd docker
docker-compose up -d
```

### Option 3: Passer les variables au démarrage

```bash
cd docker
docker-compose \
  -e MINIO_ROOT_USER=admin \
  -e MINIO_ROOT_PASSWORD=your_password \
  -e SPARK_S3A_ACCESS_KEY=admin \
  -e SPARK_S3A_SECRET_KEY=your_password \
  up -d
```

## Tables Créées dans MinIO

### Bronze Layer (données brutes)
- `bronze.customers` → `s3a://bronze/customers/`
- `bronze.orders` → `s3a://bronze/orders/`

### Silver Layer (données nettoyées)
- `silver.stg_customers` → `s3a://silver/stg_customers/`
- `silver.stg_orders` → `s3a://silver/stg_orders/`

### Gold Layer (données métier)
- `gold.customer_orders_summary` → `s3a://gold/customer_orders_summary/`
- `gold.daily_revenue` → `s3a://gold/daily_revenue/`
- `gold.top_customers` → `s3a://gold/top_customers/`

## Pipeline d'Ingestion

Le pipeline Airflow exécute les étapes suivantes:

```
1. init_tables         → Initialise les databases et tables externes (Bronze)
    ↓
2. bronze              → Ingère les CSV (customers.csv, orders.csv) → MinIO
    ↓
3. refresh_bronze      → Rafraîchit les métadonnées Hive
    ↓
4. dbt_run             → Crée les tables Silver et Gold via dbt
```

## Bonnes Pratiques

1. **Ne JAMAIS committer le fichier `.env`** - ajoutez-le à `.gitignore`
2. **Utiliser `.env.example`** comme template avec des valeurs placeholders
3. **En production**, utiliser des systèmes de gestion des secrets (AWS Secrets Manager, HashiCorp Vault, etc.)
4. **En local**, ne jamais utiliser les vraies passwords, utiliser des valeurs test
5. **Auditer les logs** pour vérifier qu'aucun credential n'est exposé

## Vérification de la Configuration

### 1. Vérifier les variables d'environnement

```bash
# Linux/Mac
env | grep -E "MINIO_|SPARK_|AIRFLOW_|DBT_"

# Windows PowerShell
Get-ChildItem env: | Where-Object {$_.Name -match "MINIO_|SPARK_|AIRFLOW_|DBT_"}
```

### 2. Tester la connexion à MinIO

```bash
# Accéder à MinIO Console
# http://localhost:9001 (admin / MINIO_ROOT_PASSWORD)
```

### 3. Tester l'initialisation des tables

```bash
cd ../
docker exec spark /opt/spark/bin/spark-submit init_tables.py
```

### 4. Afficher les tables créées

```bash
docker exec spark-thrift /opt/spark/bin/beeline \
  -u 'jdbc:hive2://spark-thrift:10000' \
   -e "SHOW DATABASES; SHOW TABLES IN bronze; SHOW TABLES IN silver; SHOW TABLES IN gold;"
```

## Dépannage

### Erreur: "Missing S3A credentials"
- Vérifier que les variables `SPARK_S3A_ACCESS_KEY` et `SPARK_S3A_SECRET_KEY` sont définies
- Vérifier le fichier `.env` est chargé par Docker Compose

### Erreur: "Connection refused" pour Spark Thrift
- Attendre que le conteneur `spark-thrift` soit prêt
- Vérifier que `DBT_SPARK_HOST` et `DBT_SPARK_PORT` sont corrects

### Tables Bronze vides après ingestion
- Vérifier que `init_tables.py` s'est exécuté avec succès
- Vérifier que les fichiers CSV existent dans `data/sources_files/`
- Consulter les logs: `docker logs spark`

