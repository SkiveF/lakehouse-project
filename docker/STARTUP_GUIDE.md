# Guide de Démarrage - Lakehouse avec Variables d'Environnement

Ce guide explique comment démarrer le lakehouse en utilisant les variables d'environnement pour les credentials.

## Sécurité Prioritaire

IMPORTANT: Ne JAMAIS hardcoder les credentials dans le code ou les fichiers de configuration. Toujours utiliser les variables d'environnement.

Consultez `SECURITY_ENV_VARS.md` pour plus de détails.

## Prérequis

- Docker Desktop installé
- Variables d'environnement définies (voir section ci-dessous)

## Démarrage Rapide

### Étape 1: Préparer les variables d'environnement

#### Option A: Fichier `.env` (Recommandé)

```bash
# À la racine du projet, créer le fichier .env
cp .env.example .env

# Modifier les passwords dans .env
# Utilisez un éditeur de texte pour éditer .env
```

**Contenu du fichier `.env`:**
```dotenv
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=your_secure_password
SPARK_S3A_ACCESS_KEY=admin
SPARK_S3A_SECRET_KEY=your_secure_password
AIRFLOW_ADMIN_PASSWORD=your_airflow_password
DBT_SPARK_HOST=spark-thrift
DBT_SPARK_PORT=10000
DBT_SPARK_USER=root
```

#### Option B: Variables d'environnement PowerShell (Windows)

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
```

### Étape 2: Démarrer Docker Compose

#### Avec fichier `.env`:

```bash
cd docker

# Linux/Mac
docker-compose --env-file ../.env up -d

# Windows PowerShell
docker-compose --env-file ..\.env up -d
```

#### Avec variables d'environnement (Windows PowerShell):

```powershell
cd docker
docker-compose up -d
```

### Étape 3: Attendre que les conteneurs soient prêts

```bash
# Vérifier l'état des conteneurs
docker-compose ps

# Attendre ~30-60 secondes pour que tous les services démarrent
# Les logs de spark-thrift indiquent quand il est prêt
docker-compose logs spark-thrift
```

### Étape 4: Initialiser les tables

```bash
# À la racine du projet
cd ../

# Initialiser la structure des tables
docker exec spark /opt/spark/bin/spark-submit init_tables.py

# Vérifier que tout est correctement créé
docker exec spark /opt/spark/bin/spark-submit validate_tables.py
```

### Étape 5: Ingérer les données (Bronze)

```bash
# Ingérer les fichiers CSV
docker exec spark /opt/spark/bin/spark-submit main.py --layer bronze

# Vérifier l'ingestion
docker exec spark /opt/spark/bin/spark-submit validate_tables.py
```

### Étape 6: Transformer les données (Silver & Gold via dbt)

```bash
# Accéder au conteneur dbt
docker exec dbt bash

# Une fois dans le conteneur:
cd /opt/project/lakehouse_dbt
dbt run

# Vérifier les résultats
dbt test
```

## Pipeline Complet via Airflow

Une fois tout initialisé, vous pouvez utiliser Airflow pour exécuter le pipeline complet.

### Accès à Airflow

- URL: http://localhost:8081
- Utilisateur: admin
- Mot de passe: Celui défini dans `AIRFLOW_ADMIN_PASSWORD`

### Déclencher le DAG

1. Accédez à http://localhost:8081
2. Naviguez vers "DAGs"
3. Trouvez "lakehouse_pipeline"
4. Cliquez sur "Trigger DAG"

Le pipeline exécutera:
1. **init_tables** → Initialise la structure
2. **bronze** → Ingère les CSV
3. **refresh_bronze** → Rafraîchit les métadonnées
4. **dbt_run** → Crée Silver et Gold

## Accès aux Interfaces Web

| Service | URL | Identifiants |
|---------|-----|--------------|
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Spark Master | http://localhost:8080 | - |
| Airflow | http://localhost:8081 | admin / `AIRFLOW_ADMIN_PASSWORD` |
| Zeppelin | http://localhost:8085 | - |
| Kafka UI | http://localhost:8086 | - |

## SQL - Requêtes Utiles

### Consulter les tables

```sql
-- Afficher toutes les databases
SHOW DATABASES;

-- Afficher les tables dans chaque layer
SHOW TABLES IN bronze;
SHOW TABLES IN silver;
SHOW TABLES IN gold;

-- Afficher la structure d'une table
DESCRIBE TABLE bronze.customers;

-- Compter les lignes
SELECT COUNT(*) FROM bronze.customers;
SELECT COUNT(*) FROM silver.stg_customers;
SELECT COUNT(*) FROM gold.customer_orders_summary;
```

### Accéder via Spark Thrift

```bash
# Utiliser beeline (client JDBC)
docker exec spark-thrift /opt/spark/bin/beeline \
  -u 'jdbc:hive2://spark-thrift:10000'

# Dans beeline:
> SHOW TABLES IN bronze;
> SELECT * FROM bronze.customers LIMIT 5;
```

## 🧹 Nettoyage

### Arrêter les conteneurs

```bash
cd docker
docker-compose down
```

### Réinitialiser complètement (supprimer les données)

```bash
# Arrêter les conteneurs et supprimer les volumes
cd docker
docker-compose down -v

# Ou nettoyer via Python
cd ../
docker exec spark /opt/spark/bin/spark-submit clean_tables.py

# Puis relancer
cd docker
docker-compose up -d
```

## Dépannage

### Erreur: "Missing S3A credentials"

**Cause**: Les variables d'environnement ne sont pas définies

**Solution**:
```bash
# Vérifier que les variables sont définies
# Linux/Mac
env | grep -E "SPARK_S3A|MINIO_ROOT"

# Windows PowerShell
Get-ChildItem env: | Where-Object {$_.Name -match "SPARK_S3A|MINIO_ROOT"}

# Ajouter au fichier .env s'il est utilisé
```

### Erreur: "Connection refused" pour Spark Thrift

**Cause**: Le conteneur `spark-thrift` n'est pas prêt

**Solution**:
```bash
# Vérifier les logs
docker logs spark-thrift

# Attendre quelques secondes et réessayer
sleep 30
docker exec spark-thrift /opt/spark/bin/beeline \
  -u 'jdbc:hive2://spark-thrift:10000' \
  -e "SHOW DATABASES;"
```

### Tables Bronze vides après ingestion

**Cause**: Les fichiers CSV ne sont pas accessibles ou ne correspondent pas aux attentes

**Solution**:
```bash
# Vérifier les fichiers
ls -la data/sources_files/

# Vérifier les logs de Spark
docker logs spark

# Réinitialiser et réessayer
docker exec spark /opt/spark/bin/spark-submit clean_tables.py
docker exec spark /opt/spark/bin/spark-submit init_tables.py
docker exec spark /opt/spark/bin/spark-submit main.py --layer bronze
```

### Ports déjà utilisés

**Cause**: Des services utilisent déjà les ports

**Solution**:
```bash
# Trouver le processus utilisant le port (exemple: 9000)
# Linux/Mac
lsof -i :9000

# Windows PowerShell
netstat -ano | findstr :9000

# Libérer le port ou utiliser des ports différents dans docker-compose.yml
```

## Documentation

- **Sécurité**: Voir `SECURITY_ENV_VARS.md`
- **Architecture**: Voir `README.md`
- **Configuration Spark**: `spark/utils.py`
- **Configuration dbt**: `lakehouse_dbt/dbt_project.yml`

## Checklist de Démarrage

- [ ] Fichier `.env` créé et rempli avec les bonnes valeurs
- [ ] Variables d'environnement définies ou chargées
- [ ] `docker-compose up -d` exécuté avec succès
- [ ] Tous les conteneurs sont en "running"
- [ ] `init_tables.py` exécuté avec succès
- [ ] `validate_tables.py` confirmé que tout est créé
- [ ] `main.py --layer bronze` a ingéré les données
- [ ] Tables Silver et Gold créées par dbt
- [ ] Airflow DAG "lakehouse_pipeline" fonctionne

---

**Prêt à commencer!**

