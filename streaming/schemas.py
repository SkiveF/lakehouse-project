"""Order schema and validation logic for streaming pipeline.

Supports dataclass typing, strict validation, and non-blocking error collection.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


ORDER_STATUSES = {"created", "paid", "shipped", "delivered", "cancelled"}
ALLOWED_CURRENCIES = {"EUR", "USD", "GBP", "CHF", "JPY"}
MIN_AMOUNT = 0.01
MIN_CUSTOMER_ID = 1


@dataclass
class Order:
	"""Represents a streaming order event."""

	order_id: str
	customer_id: int
	amount: float
	currency: str
	status: str
	created_at: str

	def to_dict(self) -> dict[str, Any]:
		return {
			"order_id": self.order_id,
			"customer_id": self.customer_id,
			"amount": self.amount,
			"currency": self.currency,
			"status": self.status,
			"created_at": self.created_at,
		}


def validate_order(order: dict[str, Any]) -> tuple[bool, str | None]:
	"""Validate an order dict. Return (is_valid, error_message).

	Non-blocking: returns False and an error message instead of raising.
	"""
	required_fields = {"order_id", "customer_id", "amount", "currency", "status", "created_at"}
	missing = required_fields - set(order)
	if missing:
		return False, f"Missing fields: {sorted(missing)}"

	if not isinstance(order["order_id"], str) or not order["order_id"]:
		return False, "order_id must be a non-empty string"
	if not isinstance(order["customer_id"], int):
		return False, "customer_id must be an integer"
	if order["customer_id"] < MIN_CUSTOMER_ID:
		return False, f"customer_id must be >= {MIN_CUSTOMER_ID}"
	if not isinstance(order["amount"], (int, float)):
		return False, "amount must be numeric"
	if order["amount"] <= 0:
		return False, f"amount must be > 0 (got {order['amount']})"
	if not isinstance(order["currency"], str) or not order["currency"]:
		return False, "currency must be a non-empty string"
	if order["currency"] not in ALLOWED_CURRENCIES:
		return False, f"currency must be one of {sorted(ALLOWED_CURRENCIES)}"
	if order["status"] not in ORDER_STATUSES:
		return False, f"status must be one of {sorted(ORDER_STATUSES)}"
	if not isinstance(order["created_at"], str):
		return False, "created_at must be an ISO-8601 string"

	try:
		datetime.fromisoformat(order["created_at"].replace("Z", "+00:00"))
	except ValueError as e:
		return False, f"Invalid ISO-8601 timestamp: {e}"

	return True, None

