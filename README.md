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
|   `-- settings.yaml               # Sources Bronze + schemas explicites
|-- spark/
|   |-- config.py                   # Chargement + validation YAML
|   `-- utils.py                    # SparkSession partagee, config S3A, purge stockage
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
|   `-- lakehouse_pipeline.py       # DAG init -> bronze -> refresh -> dbt run -> dbt test
|-- data/
|   `-- sources_files/              # CSV sources
|-- docker/
|   `-- docker-compose.yml          # Services locaux
```

## Lancement

### 0. Preparer les variables d'environnement

C'est `docker/.env` qui est lu par Docker Compose :

```powershell
Set-Location "C:\SF_DEV_EXP\lakehouse-project\docker"
Copy-Item ".env.example" ".env"
```

Puis editer `docker/.env` : credentials MinIO et Spark S3A, mot de passe Airflow,
credentials du metastore (`METASTORE_*`) et ressources allouees au Thrift Server
(`THRIFT_TOTAL_CORES`, a garder strictement inferieur au nombre de coeurs de la
machine pour que les `spark-submit` aient de quoi tourner).

Aucun prerequis externe : le metastore Hive tourne dans un service `postgres` de
la stack et les buckets MinIO sont crees automatiquement par `minio-init`. Pour
repointer le metastore vers un PostgreSQL de l'hote, il suffit de changer
`METASTORE_JDBC_URL`.

### 1. Demarrer l'infrastructure

```bash
cd docker
docker compose up -d
docker compose ps
```

Les services declarent des healthchecks et s'attendent les uns les autres
(`postgres` et `minio` avant Spark, `spark-thrift` avant dbt), donc `up -d` rend
la main quand la stack est reellement prete. Le premier demarrage prend quelques
minutes : Spark telecharge ses JARs S3A et le driver PostgreSQL.

### 2. Generer les donnees de test

```bash
python data/generate_data.py
```

### 3. Ingestion Bronze

```bash
docker exec spark /opt/spark/bin/spark-submit /opt/project/main.py --layer bronze
```

Bronze est **append-only et partitionne par `ingestion_date`** : chaque execution
ajoute une partition au lieu d'ecraser la precedente, et chaque ligne porte sa
provenance (`ingested_at`, `source_file`). La deduplication metier est faite en
Silver. Les schemas des sources sont declares dans `config/settings.yaml` : aucune
inference, et l'ingestion refuse d'ecrire si le schema declare ne correspond plus
a la table enregistree.

Options utiles :

```bash
# rejouer une ingestion sur une partition donnee
... /opt/project/main.py --layer bronze --ingestion-date 2026-09-01
# n'ingerer qu'une source
... /opt/project/main.py --layer bronze --only orders
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

Le DAG execute les etapes suivantes dans cet ordre :

1. `init_tables`
2. `bronze`
3. `refresh_bronze`
4. `dbt_run`
5. `dbt_test`

### 6. Pipeline complet en manuel

Si vous preferez lancer tout le pipeline sans Airflow :

```powershell
Set-Location "C:\SF_DEV_EXP\lakehouse-project\docker"
docker compose up -d
docker exec spark /opt/spark/bin/spark-submit /opt/project/init_tables.py
docker exec spark /opt/spark/bin/spark-submit /opt/project/main.py --layer bronze
docker exec spark-thrift /opt/spark/bin/beeline -u "jdbc:hive2://spark-thrift:10000" --silent=true -e "REFRESH TABLE bronze.customers; REFRESH TABLE bronze.orders;"
docker exec dbt dbt run --profiles-dir /opt/project/lakehouse_dbt --project-dir /opt/project/lakehouse_dbt
docker exec dbt dbt test --profiles-dir /opt/project/lakehouse_dbt --project-dir /opt/project/lakehouse_dbt
docker exec spark /opt/spark/bin/spark-submit /opt/project/validate_tables.py
```

### 7. Remettre le lakehouse a zero

```bash
docker exec spark /opt/spark/bin/spark-submit /opt/project/clean_tables.py --yes
```

Le script supprime les bases Hive **et** les donnees dans MinIO (les tables des
trois couches sont externes : les retirer du metastore ne suffit pas). Ajouter
`--metastore-only` pour ne vider que le metastore en conservant les fichiers.

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

Par defaut, `dbt run` construit tous les modeles du projet selectionnes par `dbt_project.yml`, donc ici :

- les modeles `silver`
- les modeles `gold`

Autrement dit, le bloc `dbt_run` du DAG construit bien la couche Gold, pas seulement Silver.

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

Pour ne construire que Gold :

```bash
docker exec dbt dbt run --select gold \
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

## Service dbt

Le conteneur `dbt` peut apparaitre en etat `running` en permanence. C'est normal dans cette stack.

Dans `docker/docker-compose.yml`, son processus principal installe dbt puis termine par une attente bloquante pour garder le conteneur actif. Cela permet ensuite d'executer facilement des commandes telles que `docker exec dbt dbt run`, `dbt test` ou `dbt debug` sans recreer le conteneur a chaque fois.

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
