# Lakehouse Pipeline

Pipeline de donnees en architecture Medallion (Bronze -> Silver -> Gold) avec
Apache Spark, MinIO, dbt, Apache Airflow et Apache Zeppelin, le tout lance via
Docker Compose.

## Architecture

```text
CSV sources -> Bronze raw -> Silver dbt clean -> Gold dbt aggregates
                   |              |                 |
                   +--------------+-----------------+
                                  |
                              MinIO S3
                                  |
                         Airflow orchestration
                                  |
                  Zeppelin / Spark Thrift for SQL access
```

| Couche | Description | Stockage |
|--------|-------------|----------|
| Bronze | Donnees brutes ingerees depuis les CSV avec Spark | `s3a://bronze/` |
| Silver | Donnees nettoyees et dedupliquees avec dbt | `s3a://silver/` |
| Gold | Agregations metier construites avec dbt | `s3a://gold/` |

## Stack

| Outil | Role |
|-------|------|
| Apache Spark 3.5.0 | Ingestion Bronze et execution SQL |
| MinIO | Stockage objet compatible S3 |
| Apache Airflow 2.9.1 | Orchestration du pipeline |
| dbt / dbt-spark | Transformations SQL Silver et Gold |
| Spark Thrift Server | Acces HiveServer2 pour dbt et Zeppelin |
| Apache Zeppelin | Exploration SQL |
| Docker Compose | Infrastructure locale |

## Structure

```text
lakehouse-project/
|-- main.py                         # Point d'entree Spark pour l'ingestion Bronze
|-- config/
|   `-- settings.yaml               # Sources CSV de la couche Bronze
|-- spark/
|   |-- config.py                   # Chargement YAML
|   |-- io.py                       # Helpers Parquet
|   `-- utils.py                    # SparkSession + configuration S3A
|-- ingestion/
|   `-- base_ingestion.py           # CSV -> Bronze Parquet + table Hive
|-- lakehouse_dbt/
|   |-- dbt_project.yml             # Configuration dbt
|   |-- profiles.yml                # Connexion Spark Thrift Server
|   `-- models/
|       |-- sources/                # Sources Bronze
|       |-- silver/                 # Modeles Silver
|       `-- gold/                   # Modeles Gold
|-- dags/
|   `-- lakehouse_pipeline.py       # DAG Airflow Bronze -> refresh -> dbt
|-- data/
|   `-- sources_files/              # CSV sources
|-- docker/
|   `-- docker-compose.yml          # Services locaux
`-- legacy/
    `-- transformations/            # Ancienne implementation Python/Spark Silver/Gold
```

## Lancement

### 1. Demarrer l'infrastructure

```bash
cd docker
docker compose up -d
```

### 2. Generer les donnees de test

```bash
python data/generate_data.py
```

### 3. Ingestion Bronze

```bash
docker exec spark /opt/spark/bin/spark-submit /opt/project/main.py --layer bronze
```

### 4. Transformations Silver et Gold

```bash
docker exec dbt dbt run \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

### 5. Pipeline complet via Airflow

Ouvrir [Airflow](http://localhost:8081), activer puis declencher le DAG
`lakehouse_pipeline`.

## Commandes dbt utiles

Toutes les commandes ci-dessous s'executent depuis le container `dbt` et utilisent
le projet `lakehouse_dbt`.

### Lister les modeles, sources et tests

```bash
docker exec dbt dbt ls \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

### Executer tous les modeles Silver et Gold

```bash
docker exec dbt dbt run \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

### Executer uniquement une couche ou un modele

```bash
docker exec dbt dbt run --select silver \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

```bash
docker exec dbt dbt run --select gold.daily_revenue \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

### Lancer les tests

```bash
docker exec dbt dbt test \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

### Construire modeles + tests en une commande

```bash
docker exec dbt dbt build \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

### Compiler le SQL sans executer les modeles

```bash
docker exec dbt dbt compile \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

### Verifier la configuration dbt

```bash
docker exec dbt dbt debug \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

Note : dans l'image actuelle, `dbt debug` peut signaler que `git` manque dans le
container, meme si la connexion Spark Thrift est valide.

### Nettoyer les artefacts dbt locaux

```bash
docker exec dbt dbt clean \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

## Interfaces

| Service | URL | Identifiants |
|---------|-----|--------------|
| Spark Master | [http://localhost:8080](http://localhost:8080) | - |
| Airflow | [http://localhost:8081](http://localhost:8081) | `admin` / voir `docker/.env` |
| MinIO Console | [http://localhost:9001](http://localhost:9001) | voir `docker/.env` |
| Zeppelin | [http://localhost:8085](http://localhost:8085) | - |

## Exploration SQL avec Zeppelin

Zeppelin se connecte au Spark Thrift Server via JDBC.

Configuration JDBC :

| Propriete | Valeur |
|-----------|--------|
| `default.url` | `jdbc:hive2://spark-thrift:10000` |
| `default.driver` | `org.apache.hive.jdbc.HiveDriver` |
| `default.user` | `root` |

Dependance JDBC a ajouter si necessaire :

```text
org.apache.hive:hive-jdbc:2.3.9
```

Exemples :

```sql
USE gold;
SELECT * FROM daily_revenue ORDER BY order_date DESC LIMIT 10;
```

```sql
USE gold;
SELECT * FROM top_customers;
```

## Tables Gold

### `customer_orders_summary`

| Colonne | Description |
|---------|-------------|
| `customer_id` | Identifiant client |
| `email` | Email normalise |
| `total_orders` | Nombre de commandes expediees |
| `total_amount` | Montant total des commandes expediees |
| `order_date` | Date/heure de la derniere commande expediee |

### `daily_revenue`

| Colonne | Description |
|---------|-------------|
| `order_date` | Date |
| `total_revenue` | Revenu du jour |
| `total_orders` | Nombre de commandes expediees du jour |

### `top_customers`

| Colonne | Description |
|---------|-------------|
| `customer_id` | Identifiant client |
| `total_spent` | Montant total commande |
| `total_orders` | Nombre total de commandes |
| `rank` | Position dans le classement |

## Transformations dbt

Silver :

- `stg_customers` : normalise les emails et deduplique par `customer_id`.
- `stg_orders` : deduplique par `order_id`.

Gold :

- `customer_orders_summary` : agregats client sur les commandes `shipped`.
- `daily_revenue` : revenu journalier sur les commandes `shipped`.
- `top_customers` : top 10 clients par montant total commande.

## Configuration

`config/settings.yaml` configure les sources CSV ingerees dans Bronze :

```yaml
bronze:
  sources:
    customers:
      path: /opt/project/data/sources_files/customers.csv
    orders:
      path: /opt/project/data/sources_files/orders.csv
```

Les couches Silver et Gold sont configurees dans `lakehouse_dbt/dbt_project.yml`.

## Legacy

L'ancienne implementation Python/Spark des transformations Silver et Gold est
conservee dans `legacy/transformations/` uniquement comme reference. Elle n'est
pas appelee par le pipeline Airflow/dbt actuel.

## Prerequis

- Docker Desktop avec WSL2 sous Windows
- Python 3.12+ pour generer les donnees locales
