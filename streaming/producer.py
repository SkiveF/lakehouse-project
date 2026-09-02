"""Producteur Kafka pour le topic des commandes.

Envoi unitaire (`--payload`) ou par lot (`--batch-file`). En mode lot les
messages partent de facon asynchrone et sont confirmes apres un unique
`flush()` : un aller-retour bloquant par message rendait l'envoi de milliers de
commandes inutilement lent.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from schemas import validate_order

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SEND_TIMEOUT_SECONDS = 10


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Cree un producteur qui serialise les dicts Python en JSON."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        logger.info("Producteur connecte a %s", bootstrap_servers)
        return producer
    except Exception as error:
        logger.error("Connexion au broker Kafka impossible : %s", error)
        raise


def send_message(
    producer: KafkaProducer,
    topic: str,
    payload: dict[str, Any],
    key: str | None = None,
) -> bool:
    """Envoie un message et retourne True si le broker l'a acquitte.

    Le booleen est important : sans lui, l'appelant ne peut pas distinguer un
    envoi reussi d'un echec deja journalise ici.
    """
    is_valid, error = validate_order(payload)
    if not is_valid:
        logger.error("Commande invalide : %s", error)
        return False

    try:
        encoded_key = key.encode("utf-8") if key else None
        future = producer.send(topic, key=encoded_key, value=payload)
        metadata = future.get(timeout=SEND_TIMEOUT_SECONDS)
    except KafkaTimeoutError as error:
        logger.error("Timeout Kafka : %s", error)
        return False
    except Exception as error:
        logger.error("Envoi impossible : %s", error)
        return False

    logger.info(
        "Message envoye : topic=%s partition=%s offset=%s",
        metadata.topic, metadata.partition, metadata.offset,
    )
    return True


def send_messages(
    producer: KafkaProducer,
    topic: str,
    payloads: list[dict[str, Any]],
) -> tuple[int, int]:
    """Envoie un lot en asynchrone. Retourne (envoyes, en_echec).

    Les commandes invalides sont comptees dans les echecs : elles n'atteignent
    jamais le broker.
    """
    pending: list[tuple[int, Any]] = []
    failed = 0

    for index, payload in enumerate(payloads, start=1):
        is_valid, error = validate_order(payload)
        if not is_valid:
            logger.error("[%s] invalide : %s", index, error)
            failed += 1
            continue
        try:
            future = producer.send(
                topic,
                key=(payload.get("order_id") or "").encode("utf-8") or None,
                value=payload,
            )
        except Exception as error:
            logger.error("[%s] mise en file impossible : %s", index, error)
            failed += 1
            continue
        pending.append((index, future))

    # Un seul aller-retour reseau pour tout le lot.
    producer.flush()

    sent = 0
    for index, future in pending:
        try:
            future.get(timeout=SEND_TIMEOUT_SECONDS)
            sent += 1
        except Exception as error:
            logger.error("[%s] non acquitte par le broker : %s", index, error)
            failed += 1

    logger.info("Lot termine : %s envoye(s), %s en echec", sent, failed)
    return sent, failed


def load_jsonl(path: str) -> list[dict[str, Any]]:
    """Charge un fichier JSONL en memoire, en ignorant les lignes illisibles."""
    orders: list[dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as file:
            for line_num, line in enumerate(file, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    orders.append(json.loads(line))
                except json.JSONDecodeError as error:
                    logger.error("Ligne %s : JSON invalide : %s", line_num, error)
    except FileNotFoundError:
        logger.error("Fichier introuvable : %s", path)
        raise

    logger.info("%s commande(s) chargee(s) depuis %s", len(orders), path)
    return orders


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Envoie des messages JSON vers Kafka.")
    parser.add_argument("--bootstrap-servers", default="localhost:9094")
    parser.add_argument("--topic", default="orders")
    parser.add_argument(
        "--payload",
        default=None,
        help="Payload JSON unique (utiliser --batch-file pour plusieurs commandes).",
    )
    parser.add_argument("--key", default=None, help="Cle Kafka optionnelle.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Afficher les payloads sans les envoyer.",
    )
    parser.add_argument(
        "--batch-file",
        default=None,
        help="Fichier JSONL contenant les commandes a envoyer.",
    )
    return parser.parse_args()


def run_dry_run(args: argparse.Namespace) -> int:
    if args.batch_file:
        for order in load_jsonl(args.batch_file):
            print(f"[DRY RUN] {json.dumps(order)}")
        return 0
    if args.payload:
        print(f"[DRY RUN] {json.dumps(json.loads(args.payload))}")
        return 0
    logger.warning("Ni --payload ni --batch-file : rien a faire")
    return 0


def main() -> None:
    args = parse_args()

    if args.dry_run:
        sys.exit(run_dry_run(args))

    if not args.batch_file and not args.payload:
        logger.warning("Ni --payload ni --batch-file : rien a envoyer")
        sys.exit(0)

    producer = None
    exit_code = 0
    try:
        producer = create_producer(args.bootstrap_servers)

        if args.batch_file:
            _, failed = send_messages(producer, args.topic, load_jsonl(args.batch_file))
            exit_code = 1 if failed else 0
        else:
            ok = send_message(
                producer, args.topic, json.loads(args.payload), key=args.key
            )
            exit_code = 0 if ok else 1

    except json.JSONDecodeError as error:
        logger.error("JSON invalide : %s", error)
        exit_code = 1
    except Exception as error:
        logger.error("Erreur inattendue : %s", error)
        exit_code = 1
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("Producteur ferme")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
