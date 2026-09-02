# Corrections de Sécurité - Lakehouse

## Résumé des Changes

Ce document résume toutes les modifications apportées pour éliminer les credentials en clair et utiliser les variables d'environnement.

### Fichiers Modifiés

#### 1. `streaming/consumer.py`
**Problème**: Credentials hardcodés avec valeurs par défaut `admin` / `password123`
```python
# Avant (INSÉCURISÉ)
access_key = os.getenv("SPARK_S3A_ACCESS_KEY", "admin")
secret_key = os.getenv("SPARK_S3A_SECRET_KEY", "password123")

# Après (SÉCURISÉ)
access_key = os.getenv("SPARK_S3A_ACCESS_KEY")
secret_key = os.getenv("SPARK_S3A_SECRET_KEY")
if not access_key or not secret_key:
     raise EnvironmentError("Missing S3A credentials...")
```

#### 2. `lakehouse_dbt/profiles.yml`
**Problème**: Host et port en dur
```yaml
# Avant
host: spark-thrift
port: 10000
user: root

# Après
host: "{{ env_var('DBT_SPARK_HOST', 'spark-thrift') }}"
port: "{{ env_var('DBT_SPARK_PORT', '10000') | as_number }}"
user: "{{ env_var('DBT_SPARK_USER', 'root') }}"
```

#### 3. `dags/lakehouse_pipeline.py`
**Problème**: JDBC connection string en dur à `localhost:10000`
```python
# Avant
-u 'jdbc:hive2://localhost:10000'

# Après
-u 'jdbc:hive2://{SPARK_THRIFT_HOST}:{SPARK_THRIFT_PORT}'
```

### Fichiers Créés

#### 1. `.env.example`
Template des variables d'environnement requises. À copier vers `.env` et adapter.

#### 2. `SECURITY_ENV_VARS.md`
Documentation complète sur:
- L'utilisation des variables d'environnement
- Les bonnes pratiques de sécurité
- Configuration avec Docker Compose
- Dépannage

#### 3. `init_tables.py`
Script pour initialiser la structure des tables:
- Crée les databases (bronze, silver, gold)
- Crée les tables externes Bronze
- Valide la configuration

#### 4. `validate_tables.py`
Script pour valider que toutes les tables sont créées:
- Vérifie les databases
- Vérifie les tables et leur nombre de lignes
- Fournit un rapport récapitulatif

#### 5. `clean_tables.py`
Script pour réinitialiser complètement (supprimer toutes les données):
- À utiliser pour le développement/test uniquement
- Récrée une structure vide

#### 6. `docker/STARTUP_GUIDE.md`
Guide complet de démarrage avec:
- Configuration des variables d'environnement
- Étapes de lancement
- Accès aux interfaces
- Dépannage

#### 7. `.gitignore` (mise à jour)
Amélioré pour s'assurer qu'aucun fichier `.env` ne sera jamais commité.

---

## Quick Start (Démarrage Rapide)

### 1. Configuration

```bash
# À la racine du projet
cp .env.example .env

# Éditer .env avec vos vrais passwords
```

### 2. Démarrage Docker

```bash
cd docker
docker-compose --env-file ../.env up -d
```

### 3. Initialisation

```bash
cd ../
# Initialiser la structure
docker exec spark /opt/spark/bin/spark-submit init_tables.py

# Vérifier l'initialisation
docker exec spark /opt/spark/bin/spark-submit validate_tables.py
```

### 4. Pipeline complet

```bash
# Ingestion Bronze
docker exec spark /opt/spark/bin/spark-submit main.py --layer bronze

# Transformation Silver & Gold via dbt
docker exec dbt bash -c "cd /opt/project/lakehouse_dbt && dbt run"

# Vérifier
docker exec spark /opt/spark/bin/spark-submit validate_tables.py
```

---

## Bonnes Pratiques

### À FAIRE

- [x] Utiliser les variables d'environnement pour tous les secrets
- [x] Fournir un fichier `.env.example` comme template
- [x] Documenter la configuration requise
- [x] Utiliser `.gitignore` pour les fichiers `.env`
- [x] Valider les variables d'environnement au démarrage
- [x] Fournir des messages d'erreur clairs

### À NE PAS FAIRE

- [x] Hardcoder des credentials dans le code
- [x] Committer des fichiers `.env` avec des vrais passwords
- [x] Utiliser des valeurs par défaut pour les secrets
- [x] Logger les credentials
- [x] Mettre les secrets en dur dans les fichiers YAML

---

## Variables d'Environnement Requises

| Variable | Description | Défaut | Exemple |
|----------|-------------|--------|---------|
| `MINIO_ROOT_USER` | Utilisateur MinIO | - | `admin` |
| `MINIO_ROOT_PASSWORD` | Mot de passe MinIO | - | `your_secure_password` |
| `SPARK_S3A_ACCESS_KEY` | Clé d'accès S3A | - | `admin` |
| `SPARK_S3A_SECRET_KEY` | Clé secrète S3A | - | `your_secure_password` |
| `AIRFLOW_ADMIN_PASSWORD` | Mot de passe admin Airflow | - | `your_airflow_password` |
| `DBT_SPARK_HOST` | Host Spark Thrift | `spark-thrift` | `spark-thrift` |
| `DBT_SPARK_PORT` | Port Spark Thrift | `10000` | `10000` |
| `DBT_SPARK_USER` | Utilisateur Spark | `root` | `root` |

---

## Vérification

Pour vérifier que la configuration est correcte:

```bash
# 1. Vérifier les variables (PowerShell)
Get-ChildItem env: | Where-Object {$_.Name -match "MINIO_|SPARK_|AIRFLOW_|DBT_"}

# 2. Initialiser les tables
docker exec spark /opt/spark/bin/spark-submit init_tables.py

# 3. Valider la structure
docker exec spark /opt/spark/bin/spark-submit validate_tables.py

# 4. Vérifier MinIO Console
# http://localhost:9001 (admin / MINIO_ROOT_PASSWORD)

# 5. Vérifier Spark Thrift
docker exec spark-thrift /opt/spark/bin/beeline \
  -u 'jdbc:hive2://spark-thrift:10000' \
  -e "SHOW DATABASES;"
```

---

## Support

Pour plus de détails:
- Lire `SECURITY_ENV_VARS.md`
- Lire `docker/STARTUP_GUIDE.md`
- Consulter la section "Dépannage" dans ces fichiers

---

**Tous les credentials sont maintenant sécurisés!**

