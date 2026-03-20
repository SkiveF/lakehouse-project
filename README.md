# 🏠 Lakehouse Pipeline

Pipeline de données **Medallion Architecture** (Bronze → Silver → Gold) utilisant **Apache Spark**, **MinIO** (stockage S3) et **Apache Airflow** (orchestration), le tout containerisé avec **Docker Compose**.

---

## 📐 Architecture

```
CSV Sources ──► Bronze (raw) ──► Silver (clean) ──► Gold (aggregated)
                  │                   │                   │
                  └───────── MinIO (S3-compatible) ───────┘
                                      │
                              Airflow (orchestration)
```

| Couche | Description | Stockage |
|--------|-------------|----------|
| **Bronze** | Données brutes ingérées telles quelles depuis les CSV | `s3a://bronze/` |
| **Silver** | Données nettoyées, dédupliquées, transformées | `s3a://silver/` |
| **Gold** | Agrégations métier prêtes pour l'analyse | `s3a://gold/` |

---

## 🛠️ Stack Technique

| Outil | Version | Rôle |
|-------|---------|------|
| Apache Spark | 3.5.0 | Traitement distribué des données |
| MinIO | latest | Stockage objet S3-compatible |
| Apache Airflow | 2.9.1 | Orchestration du pipeline |
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
├── dags/
│   └── lakehouse_pipeline.py        # DAG Airflow (bronze → silver → gold)
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

### 3. Lancer le pipeline complet

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/project/main.py
```

### 4. Lancer une couche spécifique

```bash
# Bronze uniquement
docker exec -it spark /opt/spark/bin/spark-submit /opt/project/main.py --layer bronze

# Silver uniquement
docker exec -it spark /opt/spark/bin/spark-submit /opt/project/main.py --layer silver

# Gold uniquement
docker exec -it spark /opt/spark/bin/spark-submit /opt/project/main.py --layer gold
```

---

## 🌐 Interfaces Web

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Airflow** | [http://localhost:8081](http://localhost:8081) | `admin` / `admin` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `admin` / `password123` |
| **Spark Master** | [http://localhost:8080](http://localhost:8080) | — |

---

## 📊 Tables Gold

### `customer_orders_summary`
Vue client enrichie avec métriques de commandes.

| Colonne | Description |
|---------|-------------|
| `customer_id` | Identifiant client |
| `email` | Email normalisé (lowercase) |
| `total_orders` | Nombre de commandes (hors annulées) |
| `total_amount` | Montant total dépensé |
| `last_order_date` | Date de la dernière commande |

### `daily_revenue`
Chiffre d'affaires journalier.

| Colonne | Description |
|---------|-------------|
| `order_date` | Date |
| `total_revenue` | Revenu du jour |
| `total_orders` | Nombre de commandes du jour |

### `top_customers`
Classement des meilleurs clients par montant dépensé.

| Colonne | Description |
|---------|-------------|
| `customer_id` | Identifiant client |
| `total_amount` | Montant total dépensé |
| `rank` | Position dans le classement |

---

## 🔄 Transformations Silver

- **`lowercase_email`** — Normalisation des emails en minuscules
- **Déduplication** — Par `primary_key` avec tri sur `updated_at` (dernière version conservée)

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

