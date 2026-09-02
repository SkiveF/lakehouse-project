# Streaming — Kafka Producer, Consumer & Order Validation

Production-ready Kafka MVP pour le pipeline Lakehouse. Génère des commandes réalistes, les envoie vers Kafka avec validation stricte, et les consomme avec Spark Structured Streaming pour écrire dans le Bronze layer.

## Architecture globale

```
┌─────────────────┐
│ generate_orders │  UUID, faker, validation (non-bloquante)
└────────┬────────┘
         ↓
┌─────────────────┐
│   producer.py   │  Logging, error handling, retry logic
└────────┬────────┘
         ↓
   [Kafka Topic]
  (orders)
         ↓
┌─────────────────┐
│  consumer.py    │  Spark Structured Streaming
└────────┬────────┘
         ↓
┌──────────────────┐
│   Bronze Layer   │  MinIO S3A (parquet + checkpoint)
└──────────────────┘
         ↓
┌──────────────────┐
│  Silver / Gold   │  dbt transformations
└──────────────────┘
```

## Fichiers

| Fichier | Rôle |
|---------|------|
| `schemas.py` | Dataclass Order + validation non-bloquante |
| `generate_orders.py` | Génère fake orders réalistes avec faker |
| `producer.py` | Envoie orders à Kafka avec retry/logging |
| `consumer.py` | Spark Structured Streaming (Kafka → Bronze) |
| `init_topics.py` | Crée automatiquement le topic Kafka |
| `tests/test_schemas.py` | Tests unitaires de validation |
| `requirements.txt` | Dépendances (kafka-python, faker) |

## Installation

```powershell
Set-Location "C:\SF_DEV_EXP\lakehouse-project"
python -m pip install -r streaming\requirements.txt
```

## Workflow complet

### 1. Initialiser le topic Kafka

```powershell
python -u streaming\init_topics.py --bootstrap-servers localhost:9094 --topics orders
```

### 2. Générer des orders

```powershell
# Afficher 5 dans la console
python -u streaming\generate_orders.py --count 5 --seed 42

# Créer un fichier JSONL
python -u streaming\generate_orders.py --count 100 --seed 42 --output streaming\orders.jsonl
```

### 3. Vérifier en dry-run

```powershell
python -u streaming\producer.py --dry-run --batch-file streaming\orders.jsonl
```

### 4. Envoyer vers Kafka

```powershell
python -u streaming\producer.py --bootstrap-servers localhost:9094 --topic orders --batch-file streaming\orders.jsonl
```

### 5. Consumer Spark (depuis le container Spark)

```bash
docker exec spark /opt/spark/bin/spark-submit \
   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
   --master spark://spark:7077 \
   /opt/project/streaming/consumer.py \
   --kafka-brokers kafka:9092 \
   --topic orders \
   --bronze-path s3a://bronze/orders/ \
   --checkpoint-path s3a://bronze/checkpoints/orders/
```

### 6. Vérifier dans Kafka UI

Ouvrir [http://localhost:8086](http://localhost:8086) et naviguer jusqu'au topic `orders` pour voir les messages.

## Validation

Chaque order doit respecter :

| Champ | Règle | Exemple |
|-------|-------|---------|
| `order_id` | UUID non-vide | `"550e8400-e29b-41d4-a716-446655440000"` |
| `customer_id` | Entier ≥ 1 | `123` |
| `amount` | Float > 0 | `99.99` |
| `currency` | EUR, USD, GBP, CHF, JPY | `"EUR"` |
| `status` | created, paid, shipped, delivered, cancelled | `"shipped"` |
| `created_at` | ISO-8601 | `"2026-07-01T10:00:00+00:00"` |

## Logging

Les trois scripts envoient des logs structurés (INFO, ERROR) :

```
2026-07-01 11:53:15,524 - __main__ - INFO - Generated 10 valid, 0 invalid orders
2026-07-01 11:53:35,180 - __main__ - INFO - Loaded 10 orders from streaming/demo.jsonl
2026-07-01 11:54:00,000 - __main__ - INFO - Message sent: topic=orders, partition=0, offset=0
2026-07-01 11:54:10,500 - __main__ - ERROR - Invalid order: amount must be > 0 (got -50.0)
```

## Tests

Lancer les tests de validation :

```powershell
python -u streaming\tests\test_schemas.py
```

Tests inclus :
- Valid order
- Amount > 0
- Customer ID >= 1
- Currency in whitelist
- Status in whitelist
- Missing fields detection
- Invalid ISO-8601
- All currencies accepted
- All statuses accepted

## Configuration MinIO (Bronze)

Pour envoyer vers MinIO depuis Spark, les variables d'environnement ou spark-submit options doivent inclure :

```bash
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
--conf spark.hadoop.fs.s3a.access.key=minioadmin \
--conf spark.hadoop.fs.s3a.secret.key=minioadmin \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
```

## Prochaines étapes

1. Producer / Consumer
2. Validation + Logging
3. Tests unitaires
4. Intégration Airflow DAG pour orchestrer le pipeline complet
5. Connecter les modèles dbt Silver/Gold au flux streaming
6. Monitoring + Alertes Kafka




