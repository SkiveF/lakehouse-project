"""Tests des fonctions pures de l'ingestion Bronze.

Les tests qui necessitent une SparkSession sont hors de ce fichier : ici on ne
teste que ce qui est verifiable sans cluster.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

pytest.importorskip("pyspark", reason="pyspark requis pour importer le module d'ingestion")

from ingestion.base_ingestion import (  # noqa: E402
    METADATA_COLUMNS,
    PARTITION_COLUMN,
    build_schema_ddl,
)


def test_ddl_respecte_l_ordre_et_echappe_les_noms():
    ddl = build_schema_ddl(
        [
            {"name": "order_id", "type": "int"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "amount", "type": "double"},
        ]
    )
    assert ddl == "`order_id` int, `timestamp` timestamp, `amount` double"


def test_ddl_accepte_les_types_parametres():
    ddl = build_schema_ddl([{"name": "montant", "type": "decimal(10,2)"}])
    assert ddl == "`montant` decimal(10,2)"


def test_schema_vide_refuse():
    with pytest.raises(ValueError, match="Schema vide"):
        build_schema_ddl([])


def test_colonne_de_partition_declaree_comme_metadonnee():
    """La colonne de partition doit etre exclue de la comparaison de schema metier."""
    assert PARTITION_COLUMN in METADATA_COLUMNS


def test_metadonnees_sans_prefixe_underscore():
    """Spark ignore les repertoires commencant par '_' : les colonnes techniques
    ne doivent jamais en porter, sinon la partition devient invisible."""
    assert all(not name.startswith("_") for name in METADATA_COLUMNS)
