"""End-to-end integration test for the streaming pipeline."""

import sys
from pathlib import Path

# Add parent directory to path
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

import schemas

validate_order = schemas.validate_order
ALLOWED_CURRENCIES = schemas.ALLOWED_CURRENCIES
ORDER_STATUSES = schemas.ORDER_STATUSES


def test_integration():
    """Run a simple integration test without Kafka/Spark."""
    print("\n" + "="*60)
    print("STREAMING PIPELINE INTEGRATION TEST")
    print("="*60 + "\n")

    # Test 1: Schema validation
    print("Test 1: Schema validation")
    test_order = {
        "order_id": "test-123",
        "customer_id": 1,
        "amount": 99.99,
        "currency": "EUR",
        "status": "shipped",
        "created_at": "2026-07-01T10:00:00+00:00",
    }
    is_valid, error = validate_order(test_order)
    assert is_valid, f"Validation failed: {error}"
    print(f"  → Order validated: {test_order}")

    # Test 2: Invalid order detection
    print("\nTest 2: Invalid order detection")
    invalid_order = {
        "order_id": "test-456",
        "customer_id": 0,  # Invalid: must be >= 1
        "amount": -50,  # Invalid: must be > 0
        "currency": "XYZ",  # Invalid: not in whitelist
        "status": "unknown",  # Invalid: not in enum
        "created_at": "invalid-date",  # Invalid: not ISO-8601
    }
    is_valid, error = validate_order(invalid_order)
    assert not is_valid, "Should have detected invalid order"
    print(f"  → Error correctly detected: {error}")

    # Test 3: All currencies and statuses
    print("\nTest 3: Currency and status whitelist")
    print(f"  → Allowed currencies: {sorted(ALLOWED_CURRENCIES)}")
    print(f"  → Allowed statuses: {sorted(ORDER_STATUSES)}")

    print("\n" + "="*60)
    print("ALL INTEGRATION TESTS PASSED")
    print("="*60 + "\n")
    print("Next steps:")
    print("1. docker compose up -d kafka kafka-ui")
    print("2. python -u streaming/generate_orders.py --count 100 --output streaming/orders.jsonl")
    print("3. python -u streaming/producer.py --bootstrap-servers localhost:9094 --topic orders --batch-file streaming/orders.jsonl")
    print("4. Open http://localhost:8086 to view messages in Kafka UI")
    print("5. Launch Spark consumer from spark container")
    print("\nSee streaming/README.md for full instructions.")


if __name__ == "__main__":
    try:
        test_integration()
    except AssertionError as e:
        print(f"\nTEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e}")
        sys.exit(1)

