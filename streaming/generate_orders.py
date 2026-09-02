import argparse
import json
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Iterable

from faker import Faker

from schemas import ALLOWED_CURRENCIES, ORDER_STATUSES, validate_order

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

fake = Faker()


def generate_order() -> dict:
	"""Generate a realistic fake order using faker."""
	return {
		"order_id": str(uuid.uuid4()),
		"customer_id": random.randint(1, 500),
		"amount": round(random.uniform(10.0, 950.0), 2),
		"currency": random.choice(list(ALLOWED_CURRENCIES)),
		"status": random.choice(list(ORDER_STATUSES)),
		"created_at": datetime.now(timezone.utc).isoformat(),
	}


def generate_orders(count: int) -> list[dict]:
	return [generate_order() for _ in range(count)]


def write_jsonl(orders: Iterable[dict], output_path: str) -> None:
	with open(output_path, "w", encoding="utf-8") as file:
		for order in orders:
			file.write(json.dumps(order) + "\n")


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Generate fake orders as JSON lines.")
	parser.add_argument("--count", type=int, default=1000, help="Number of orders to generate.")
	parser.add_argument("--seed", type=int, default=None, help="Optional random seed.")
	parser.add_argument(
		"--output",
		default=None,
		help="Optional output path (JSONL). If omitted, prints to stdout.",
	)
	return parser.parse_args()


def main() -> None:
	args = parse_args()

	if args.count <= 0:
		logger.error("--count must be > 0")
		raise ValueError("--count must be > 0")

	if args.seed is not None:
		random.seed(args.seed)
		Faker.seed(args.seed)

	orders = generate_orders(args.count)

	# Validate all orders, collecting errors but continuing.
	valid_count = 0
	invalid_count = 0
	for idx, order in enumerate(orders, start=1):
		is_valid, error = validate_order(order)
		if not is_valid:
			logger.error(f"Order {idx}: {error}")
			invalid_count += 1
		else:
			valid_count += 1

	logger.info(f"Generated {valid_count} valid, {invalid_count} invalid orders")

	if args.output:
		write_jsonl(orders, args.output)
		logger.info(f"Wrote {len(orders)} orders to {args.output}")
		return

	for order in orders:
		print(json.dumps(order))


if __name__ == "__main__":
	main()

