import argparse
import json
import logging
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from schemas import validate_order

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_producer(bootstrap_servers: str) -> KafkaProducer:
	"""Create a Kafka producer that serializes Python dicts as JSON bytes."""
	try:
		producer = KafkaProducer(
			bootstrap_servers=bootstrap_servers,
			value_serializer=lambda value: json.dumps(value).encode("utf-8"),
		)
		logger.info(f"Kafka producer connected to {bootstrap_servers}")
		return producer
	except Exception as e:
		logger.error(f"Failed to connect to Kafka broker: {e}")
		raise


def send_message(
	producer: KafkaProducer,
	topic: str,
	payload: dict[str, Any],
	key: str | None = None,
) -> None:
	"""Send a single message to Kafka with validation and error handling."""
	is_valid, error = validate_order(payload)
	if not is_valid:
		logger.error(f"Invalid order: {error}")
		return

	try:
		encoded_key = key.encode("utf-8") if key else None
		future = producer.send(topic, key=encoded_key, value=payload)
		metadata = future.get(timeout=10)
		logger.info(
			f"Message sent: topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset}"
		)
	except KafkaTimeoutError as e:
		logger.error(f"Kafka timeout: {e}")
	except Exception as e:
		logger.error(f"Failed to send message: {e}")


def send_messages(
	producer: KafkaProducer,
	topic: str,
	payloads: list[dict[str, Any]],
) -> None:
	"""Send multiple messages, logging errors but continuing."""
	valid_count = 0
	invalid_count = 0
	for index, payload in enumerate(payloads, start=1):
		is_valid, error = validate_order(payload)
		if not is_valid:
			logger.error(f"[{index}] Invalid: {error}")
			invalid_count += 1
			continue
		try:
			send_message(producer, topic, payload, key=payload.get("order_id"))
			valid_count += 1
		except Exception as e:
			logger.error(f"[{index}] Send failed: {e}")
			invalid_count += 1

	logger.info(f"Batch complete: {valid_count} sent, {invalid_count} failed")


def load_jsonl(path: str) -> list[dict[str, Any]]:
	"""Load all lines from a JSONL file into memory."""
	orders = []
	try:
		with open(path, "r", encoding="utf-8") as file:
			for line_num, line in enumerate(file, start=1):
				line = line.strip()
				if not line:
					continue
				try:
					orders.append(json.loads(line))
				except json.JSONDecodeError as e:
					logger.error(f"Line {line_num}: Invalid JSON: {e}")
		logger.info(f"Loaded {len(orders)} orders from {path}")
		return orders
	except FileNotFoundError:
		logger.error(f"File not found: {path}")
		raise


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Send JSON messages to Kafka.")
	parser.add_argument("--bootstrap-servers", default="localhost:9094")
	parser.add_argument("--topic", default="orders")
	parser.add_argument(
		"--payload",
		default=None,
		help="JSON string payload (optional; use --batch-file for multiple orders).",
	)
	parser.add_argument("--key", default=None, help="Optional Kafka message key.")
	parser.add_argument(
		"--dry-run",
		action="store_true",
		help="Print payloads without sending to Kafka.",
	)
	parser.add_argument(
		"--batch-file",
		default=None,
		help="JSONL file with orders to send.",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	if args.dry_run:
		if args.batch_file:
			orders = load_jsonl(args.batch_file)
			for order in orders:
				print(f"[DRY RUN] {json.dumps(order)}")
			return

		if args.payload:
			payload = json.loads(args.payload)
			print(f"[DRY RUN] {json.dumps(payload)}")
		else:
			logger.warning("No payload or batch file provided; nothing to do")
		return

	producer = None
	try:
		producer = create_producer(args.bootstrap_servers)

		if args.batch_file:
			orders = load_jsonl(args.batch_file)
			send_messages(producer, args.topic, orders)
		elif args.payload:
			payload = json.loads(args.payload)
			send_message(producer, args.topic, payload, key=args.key)
		else:
			logger.warning("No payload or batch file provided; nothing to send")

	except json.JSONDecodeError as e:
		logger.error(f"Invalid JSON: {e}")
	except Exception as e:
		logger.error(f"Unexpected error: {e}")
	finally:
		if producer:
			producer.flush()
			producer.close()
			logger.info("Producer closed")


if __name__ == "__main__":
	main()


