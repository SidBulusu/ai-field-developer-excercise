import pytest
from pydantic import ValidationError

from mcp_server.schemas import CustomerLookup, RefundRequest


@pytest.mark.parametrize(
    "customer_id",
    ["12345", "CUST-1234", "CUST-123456", "CUST-ABCDE", "CUST-１２３４５",
     "CUST-12345\n", " CUST-12345", 12345, True, None, [], {}],
)
def test_reject_malformed_customer_ids(customer_id):
    with pytest.raises(ValidationError):
        CustomerLookup(customer_id=customer_id)


@pytest.mark.parametrize("arguments", [{}, {"customer_id": "CUST-12345", "extra": 1}])
def test_lookup_requires_exact_fields(arguments):
    with pytest.raises(ValidationError):
        CustomerLookup.model_validate(arguments)


@pytest.mark.parametrize("amount", [0, -1, float("nan"), float("inf"), -float("inf"), True, False, "12.5", None, [], {}])
def test_reject_invalid_amounts(amount):
    with pytest.raises(ValidationError):
        RefundRequest(customer_id="CUST-12345", amount=amount, reason="Duplicate charge")


@pytest.mark.parametrize("reason", ["", "short", " 123456789 ", " " * 20, True, 1234567890, None, [], {}])
def test_reject_invalid_reasons(reason):
    with pytest.raises(ValidationError):
        RefundRequest(customer_id="CUST-12345", amount=1, reason=reason)


@pytest.mark.parametrize("missing", ["customer_id", "amount", "reason"])
def test_refund_requires_all_fields(missing):
    arguments = dict(customer_id="CUST-12345", amount=1, reason="Duplicate charge")
    del arguments[missing]
    with pytest.raises(ValidationError):
        RefundRequest.model_validate(arguments)


def test_refund_rejects_extra_fields():
    with pytest.raises(ValidationError):
        RefundRequest(customer_id="CUST-12345", amount=1, reason="Duplicate charge", extra=1)


@pytest.mark.parametrize("amount", [1, 12.5])
def test_valid_refund_trims_reason_and_accepts_json_numbers(amount):
    request = RefundRequest(customer_id="CUST-12345", amount=amount, reason="  1234567890  ")
    assert request.reason == "1234567890"
    assert request.amount == amount


def test_discovery_schema_constraints():
    schema = RefundRequest.model_json_schema()
    assert schema["additionalProperties"] is False
    assert set(schema["required"]) == {"customer_id", "amount", "reason"}
    assert schema["properties"]["customer_id"]["pattern"] == r"^CUST-[0-9]{5}$"
    assert schema["properties"]["amount"]["exclusiveMinimum"] == 0
    assert schema["properties"]["reason"]["minLength"] == 10
