# 🏠 Lakehouse Pipeline

Pipeline de données **Medallion Architecture** (Bronze → Silver → Gold) utilisant **Apache Spark**, **MinIO** (stockage S3), **dbt** (transformations SQL), **Apache Airflow** (orchestration) et **Apache Zeppelin** (exploration SQL), le tout containerisé avec **Docker Compose**.

---

## 📐 Architecture

```
CSV Sources ──► Bronze (raw) ──► Silver (dbt clean) ──► Gold (dbt aggregated)
                  │                     │                        │
                  └──────────── MinIO (S3-compatible) ───────────┘
                                        │
                         ┌──────────────┴──────────────┐
                    Airflow (orchestration)     Zeppelin / Superset (SQL)
```

| Couche | Description | Stockage |
|--------|-------------|----------|
| **Bronze** | Données brutes ingérées telles quelles depuis les CSV | `s3a://bronze/` |
| **Silver** | Données nettoyées, dédupliquées, transformées (dbt) | `s3a://silver/` |
| **Gold** | Agrégations métier prêtes pour l'analyse (dbt) | `s3a://gold/` |

---

## 🛠️ Stack Technique

| Outil | Version | Rôle |
|-------|---------|------|
| Apache Spark | 3.5.0 | Traitement distribué des données |
| MinIO | latest | Stockage objet S3-compatible |
| Apache Airflow | 2.9.1 | Orchestration du pipeline |
| dbt (dbt-spark) | 1.x | Transformations Silver & Gold en SQL |
| Spark Thrift Server | 3.5.0 | Exposition HiveServer2 (JDBC/ODBC) |
| Apache Zeppelin | 0.11.1 | Notebooks SQL interactifs |
| Docker Compose | - | Containerisation de l'infrastructure |
| Python | 3.12+ | Langage du pipeline |

---

## 📁 Structure du Projet

```
lakehouse-project/
├── main.py                          # Point d'entrée du pipeline
├── config/
│   └── settings.yaml                # Configuration des sources et chemins
├── spark/
│   ├── config.py                    # Chargement YAML
│   ├── io.py                        # Lecture bronze/silver (Parquet)
│   └── utils.py                     # Création SparkSession + config S3A
├── ingestion/
│   └── base_ingestion.py            # Lecture CSV → écriture bronze
├── transformations/
│   ├── bronze_to_silver.py          # Nettoyage, déduplication
│   └── silver_to_gold.py            # Agrégations métier
├── lakehouse_dbt/
│   ├── dbt_project.yml              # Configuration dbt
│   ├── profiles.yml                 # Connexion Spark Thrift Server
│   └── models/
│       ├── sources/sources.yml      # Déclaration des sources bronze
│       ├── silver/                  # Modèles Silver (stg_customers, stg_orders)
│       └── gold/                    # Modèles Gold (daily_revenue, top_customers, customer_orders_summary)
├── dags/
│   └── lakehouse_pipeline.py        # DAG Airflow (bronze → dbt silver → dbt gold)
├── data/
│   ├── sources_files/               # Fichiers CSV sources
│   └── generate_data.py             # Script de génération de données
├── docker/
│   └── docker-compose.yml           # Infrastructure containerisée
└── README.md
```

---

## 🚀 Lancement

### 1. Démarrer l'infrastructure

```bash
cd docker
docker compose up -d
```

### 2. Générer les données de test

```bash
python data/generate_data.py
```

### 3. Ingestion Bronze (Spark)

```bash
docker exec spark /opt/spark/bin/spark-submit /opt/project/main.py --layer bronze
```

### 4. Transformations Silver & Gold (dbt)

```bash
docker exec dbt dbt run \
  --profiles-dir /opt/project/lakehouse_dbt \
  --project-dir /opt/project/lakehouse_dbt
```

### 5. Pipeline complet via Airflow

Accède à [http://localhost:8081](http://localhost:8081) → active et déclenche le DAG `lakehouse_pipeline`.

---

## 🌐 Interfaces Web

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Spark Master** | [http://localhost:8080](http://localhost:8080) | — |
| **Airflow** | [http://localhost:8081](http://localhost:8081) | `admin` / voir `docker/.env` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | voir `docker/.env` |
| **Zeppelin** | [http://localhost:8085](http://localhost:8085) | — |

---

## 🔍 Exploration SQL avec Zeppelin

Zeppelin se connecte au **Spark Thrift Server** via JDBC.

### Configuration de l'interpréteur JDBC (une seule fois)

1. Menu **Interpreter** → recherche `jdbc` → **Edit**
2. Renseigner :

| Propriété | Valeur |
|-----------|--------|
| `default.url` | `jdbc:hive2://spark-thrift:10000` |
| `default.driver` | `org.apache.hive.jdbc.HiveDriver` |
| `default.user` | `root` |

3. **Dependencies** → ajouter : `org.apache.hive:hive-jdbc:2.3.9`
4. **Save** → **Restart**

### Exemple de notebook

```sql
%jdbc
USE gold;
SELECT * FROM daily_revenue ORDER BY order_date DESC LIMIT 10;
```

```sql
%jdbc
USE gold;
SELECT * FROM top_customers;
```

---

## 📊 Tables Gold

### `customer_orders_summary`

| Colonne | Description |
|---------|-------------|
| `customer_id` | Identifiant client |
| `email` | Email normalisé (lowercase) |
| `total_orders` | Nombre de commandes (hors annulées) |
| `total_amount` | Montant total dépensé |
| `last_order_date` | Date de la dernière commande |

### `daily_revenue`

| Colonne | Description |
|---------|-------------|
| `order_date` | Date |
| `total_revenue` | Revenu du jour |
| `total_orders` | Nombre de commandes du jour |

### `top_customers`

| Colonne | Description |
|---------|-------------|
| `customer_id` | Identifiant client |
| `total_amount` | Montant total dépensé |
| `rank` | Position dans le classement |

---

## 🔄 Transformations Silver (dbt)

- **`stg_customers`** — Normalisation email (lowercase), déduplication par `customer_id`
- **`stg_orders`** — Filtrage des statuts invalides, déduplication par `order_id`

---

## ⚙️ Configuration

Toute la configuration est centralisée dans `config/settings.yaml` :

```yaml
bronze:
  sources:
    customers:
      path: /opt/project/data/sources_files/customers.csv
    orders:
      path: /opt/project/data/sources_files/orders.csv

silver:
  sources:
    customers:
      path: s3a://bronze/customers/
      primary_key: customer_id
      order_by: updated_at
      transformations:
        - lowercase_email
    orders:
      path: s3a://bronze/orders/
      primary_key: order_id
      order_by: updated_at
```

---

## 📝 Prérequis

- **Docker Desktop** (avec WSL2 sous Windows)
- **Python 3.12+** (pour la génération de données locale)

