"""Chargement et validation de la configuration du pipeline."""

from __future__ import annotations

from typing import Any

import yaml


def load_config(path: str) -> dict[str, Any]:
    """Charge un fichier YAML de configuration."""
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def validate_bronze_config(config: dict[str, Any]) -> dict[str, Any]:
    """Valide la section bronze et retourne ses sources.

    Verifie que chaque source declare un chemin et un schema explicite, afin
    qu'une erreur de configuration echoue ici plutot qu'au milieu d'un job Spark.
    """
    bronze = config.get("bronze")
    if not isinstance(bronze, dict):
        raise ValueError("Configuration invalide : section 'bronze' manquante")

    sources = bronze.get("sources")
    if not isinstance(sources, dict) or not sources:
        raise ValueError("Configuration invalide : 'bronze.sources' vide ou absent")

    for name, source in sources.items():
        if not source.get("path"):
            raise ValueError(f"Source '{name}' : 'path' manquant")
        if not source.get("schema"):
            raise ValueError(
                f"Source '{name}' : 'schema' manquant. Les schemas sont "
                "obligatoires (pas d'inference) pour eviter la derive."
            )
        for column in source["schema"]:
            if not column.get("name") or not column.get("type"):
                raise ValueError(
                    f"Source '{name}' : chaque colonne doit avoir 'name' et 'type'"
                )

    return sources
