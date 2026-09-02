"""Tests de la validation de configuration (aucune dependance Spark)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from spark.config import validate_bronze_config  # noqa: E402


def valid_config() -> dict:
    return {
        "bronze": {
            "root": "s3a://bronze",
            "sources": {
                "orders": {
                    "path": "/opt/project/data/sources_files/orders.csv",
                    "schema": [
                        {"name": "order_id", "type": "int"},
                        {"name": "amount", "type": "double"},
                    ],
                }
            },
        }
    }


def test_config_valide_retourne_les_sources():
    sources = validate_bronze_config(valid_config())
    assert list(sources) == ["orders"]
    assert sources["orders"]["path"].endswith("orders.csv")


def test_section_bronze_manquante():
    with pytest.raises(ValueError, match="bronze"):
        validate_bronze_config({})


def test_sources_vides():
    with pytest.raises(ValueError, match="sources"):
        validate_bronze_config({"bronze": {"sources": {}}})


def test_path_manquant():
    config = valid_config()
    del config["bronze"]["sources"]["orders"]["path"]
    with pytest.raises(ValueError, match="path"):
        validate_bronze_config(config)


def test_schema_obligatoire():
    """Un schema absent doit echouer : c'est ce qui empeche le retour de l'inference."""
    config = valid_config()
    del config["bronze"]["sources"]["orders"]["schema"]
    with pytest.raises(ValueError, match="schema"):
        validate_bronze_config(config)


def test_colonne_sans_type():
    config = valid_config()
    config["bronze"]["sources"]["orders"]["schema"] = [{"name": "order_id"}]
    with pytest.raises(ValueError, match="type"):
        validate_bronze_config(config)


def test_settings_reel_est_valide():
    """Le settings.yaml du depot doit passer la validation."""
    yaml = pytest.importorskip("yaml")
    root = Path(__file__).resolve().parent.parent
    config = yaml.safe_load((root / "config" / "settings.yaml").read_text(encoding="utf-8"))
    sources = validate_bronze_config(config)
    assert set(sources) == {"customers", "orders"}
