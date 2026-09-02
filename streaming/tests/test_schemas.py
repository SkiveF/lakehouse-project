"""Unit tests for streaming schema validation."""

import sys
from pathlib import Path

# Add parent directory to path so we can import schemas
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

import schemas

validate_order = schemas.validate_order
ALLOWED_CURRENCIES = schemas.ALLOWED_CURRENCIES
ORDER_STATUSES = schemas.ORDER_STATUSES


def test_valid_order():
    """Test a valid order passes validation."""
    order = {
        "order_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": 123,
        "amount": 99.99,
        "currency": "EUR",
        "status": "shipped",
        "created_at": "2026-07-01T10:00:00+00:00",
    }
    is_valid, error = validate_order(order)
    assert is_valid is True, f"Expected valid, got error: {error}"
    assert error is None


def test_invalid_amount_zero():
    """Test that amount must be > 0."""
    order = {
        "order_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": 123,
        "amount": 0,
        "currency": "EUR",
        "status": "shipped",
        "created_at": "2026-07-01T10:00:00+00:00",
    }
    is_valid, error = validate_order(order)
    assert is_valid is False
    assert "amount must be > 0" in error


def test_invalid_amount_negative():
    """Test that amount must be positive."""
    order = {
        "order_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": 123,
        "amount": -50.0,
        "currency": "EUR",
        "status": "shipped",
        "created_at": "2026-07-01T10:00:00+00:00",
    }
    is_valid, error = validate_order(order)
    assert is_valid is False
    assert "amount must be > 0" in error


def test_invalid_customer_id_zero():
    """Test that customer_id must be >= 1."""
    order = {
        "order_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": 0,
        "amount": 99.99,
        "currency": "EUR",
        "status": "shipped",
        "created_at": "2026-07-01T10:00:00+00:00",
    }
    is_valid, error = validate_order(order)
    assert is_valid is False
    assert "customer_id must be >= 1" in error


def test_invalid_customer_id_negative():
    """Test that customer_id must be positive."""
    order = {
        "order_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": -1,
        "amount": 99.99,
        "currency": "EUR",
        "status": "shipped",
        "created_at": "2026-07-01T10:00:00+00:00",
    }
    is_valid, error = validate_order(order)
    assert is_valid is False
    assert "customer_id must be >= 1" in error


def test_invalid_currency():
    """Test that currency must be in allowed list."""
    order = {
        "order_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": 123,
        "amount": 99.99,
        "currency": "XYZ",
        "status": "shipped",
        "created_at": "2026-07-01T10:00:00+00:00",
    }
    is_valid, error = validate_order(order)
    assert is_valid is False
    assert "currency must be one of" in error


def test_invalid_status():
    """Test that status must be in allowed list."""
    order = {
        "order_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": 123,
        "amount": 99.99,
        "currency": "EUR",
        "status": "unknown",
        "created_at": "2026-07-01T10:00:00+00:00",
    }
    is_valid, error = validate_order(order)
    assert is_valid is False
    assert "status must be one of" in error


def test_missing_field():
    """Test that missing required fields are detected."""
    order = {
        "order_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": 123,
        "amount": 99.99,
        # Missing currency, status, created_at
    }
    is_valid, error = validate_order(order)
    assert is_valid is False
    assert "Missing fields" in error


def test_invalid_timestamp():
    """Test that invalid ISO-8601 timestamps are rejected."""
    order = {
        "order_id": "550e8400-e29b-41d4-a716-446655440000",
        "customer_id": 123,
        "amount": 99.99,
        "currency": "EUR",
        "status": "shipped",
        "created_at": "not-a-timestamp",
    }
    is_valid, error = validate_order(order)
    assert is_valid is False
    assert "Invalid ISO-8601" in error


def test_all_currencies():
    """Test that all allowed currencies are accepted."""
    for currency in ALLOWED_CURRENCIES:
        order = {
            "order_id": "550e8400-e29b-41d4-a716-446655440000",
            "customer_id": 123,
            "amount": 99.99,
            "currency": currency,
            "status": "shipped",
            "created_at": "2026-07-01T10:00:00+00:00",
        }
        is_valid, error = validate_order(order)
        assert is_valid is True, f"Currency {currency} should be valid: {error}"


def test_all_statuses():
    """Test that all allowed statuses are accepted."""
    for status in ORDER_STATUSES:
        order = {
            "order_id": "550e8400-e29b-41d4-a716-446655440000",
            "customer_id": 123,
            "amount": 99.99,
            "currency": "EUR",
            "status": status,
            "created_at": "2026-07-01T10:00:00+00:00",
        }
        is_valid, error = validate_order(order)
        assert is_valid is True, f"Status {status} should be valid: {error}"


if __name__ == "__main__":
    # Run tests manually
    test_valid_order()
    test_invalid_amount_zero()
    test_invalid_amount_negative()
    test_invalid_customer_id_zero()
    test_invalid_customer_id_negative()
    test_invalid_currency()
    test_invalid_status()
    test_missing_field()
    test_invalid_timestamp()
    test_all_currencies()
    test_all_statuses()

    print("All tests passed!")

