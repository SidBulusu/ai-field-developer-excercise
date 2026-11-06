"""Strict, flat input schemas shared by discovery and execution."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints


# Use ASCII digits: \d would also accept Unicode digits outside the ID convention.
CustomerID = Annotated[str, StringConstraints(strict=True, pattern=r"^CUST-[0-9]{5}$")]
# Padding must not let an otherwise short reason satisfy the minimum length.
RefundReason = Annotated[
    str, StringConstraints(strict=True, strip_whitespace=True, min_length=10)
]


class CustomerLookup(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    customer_id: CustomerID


class RefundRequest(CustomerLookup):
    # JSON has one numeric type: accept integers, but exclude bools and numeric strings.
    amount: Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]
    reason: RefundReason
