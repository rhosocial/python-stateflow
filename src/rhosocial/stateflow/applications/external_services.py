# src/rhosocial/stateflow/applications/external_services.py
"""External service primitives and mock implementations.

stateflow is a workflow framework, not a payment or billing system. These
classes define **protocols** (interfaces) for external services that
applications commonly need, plus **mock implementations** for testing and
local development. Production users inject their own concrete services.

Sync and async variants are provided, following the stateflow parity
principle — they are parallel and non-interoperable.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------


class PaymentService(ABC):
    """Protocol for payment / refund operations.

    In a real system this wraps a gateway like Stripe, Alipay, etc.
    """

    @abstractmethod
    def charge(self, order_id: Any, amount: int, currency: str = "CNY") -> str:
        """Charge ``amount`` (minor units) and return a transaction id."""

    @abstractmethod
    def refund(self, transaction_id: str) -> bool:
        """Refund a previous charge. Returns True on success."""

    @abstractmethod
    def get_status(self, transaction_id: str) -> str:
        """Return the status of a transaction: 'pending', 'succeeded', 'failed'."""


class CreditService(ABC):
    """Protocol for credit / points operations.

    In a real system this wraps an internal points ledger.
    """

    @abstractmethod
    def freeze(self, user_id: str, amount: int, reason: str = "") -> str:
        """Freeze ``amount`` credits (pre-deduction). Returns a freeze id."""

    @abstractmethod
    def commit(self, freeze_id: str) -> bool:
        """Commit a previous freeze, permanently deducting the credits."""

    @abstractmethod
    def unfreeze(self, freeze_id: str) -> bool:
        """Release a previous freeze, returning credits to the user."""

    @abstractmethod
    def get_balance(self, user_id: str) -> int:
        """Return the available (unfrozen) credit balance for a user."""


class AsyncPaymentService(ABC):
    """Async protocol for payment / refund operations."""

    @abstractmethod
    async def charge(self, order_id: Any, amount: int, currency: str = "CNY") -> str:
        """Charge ``amount`` (minor units) and return a transaction id."""

    @abstractmethod
    async def refund(self, transaction_id: str) -> bool:
        """Refund a previous charge. Returns True on success."""

    @abstractmethod
    async def get_status(self, transaction_id: str) -> str:
        """Return the status of a transaction."""


class AsyncCreditService(ABC):
    """Async protocol for credit / points operations."""

    @abstractmethod
    async def freeze(self, user_id: str, amount: int, reason: str = "") -> str:
        """Freeze ``amount`` credits. Returns a freeze id."""

    @abstractmethod
    async def commit(self, freeze_id: str) -> bool:
        """Commit a previous freeze."""

    @abstractmethod
    async def unfreeze(self, freeze_id: str) -> bool:
        """Release a previous freeze."""

    @abstractmethod
    async def get_balance(self, user_id: str) -> int:
        """Return the available credit balance for a user."""


# ---------------------------------------------------------------------------
# Mock implementations (sync)
# ---------------------------------------------------------------------------


class MockPaymentService(PaymentService):
    """In-memory mock for testing and local development.

    All charges succeed by default. To simulate a failure, pre-set
    ``fail_next_charge = True`` or add a transaction id to ``failed_refunds``.
    """

    def __init__(self) -> None:
        self._transactions: Dict[str, Dict[str, Any]] = {}
        self._counter = 0
        self.fail_next_charge: bool = False

    def charge(self, order_id: Any, amount: int, currency: str = "CNY") -> str:
        if self.fail_next_charge:
            self.fail_next_charge = False
            tx_id = f"mock-tx-{self._counter}"
            self._counter += 1
            self._transactions[tx_id] = {
                "order_id": order_id, "amount": amount,
                "currency": currency, "status": "failed",
            }
            return tx_id
        tx_id = f"mock-tx-{self._counter}"
        self._counter += 1
        self._transactions[tx_id] = {
            "order_id": order_id, "amount": amount,
            "currency": currency, "status": "succeeded",
        }
        return tx_id

    def refund(self, transaction_id: str) -> bool:
        tx = self._transactions.get(transaction_id)
        if tx is None or tx["status"] != "succeeded":
            return False
        tx["status"] = "refunded"
        return True

    def get_status(self, transaction_id: str) -> str:
        tx = self._transactions.get(transaction_id)
        return tx["status"] if tx else "unknown"


class MockCreditService(CreditService):
    """In-memory mock credit ledger.

    Balances are pre-seeded via ``set_balance(user_id, amount)``.
    """

    def __init__(self) -> None:
        self._balances: Dict[str, int] = {}
        self._freezes: Dict[str, Dict[str, Any]] = {}
        self._counter = 0

    def set_balance(self, user_id: str, amount: int) -> None:
        """Seed a user's balance for testing."""
        self._balances[user_id] = amount

    def freeze(self, user_id: str, amount: int, reason: str = "") -> str:
        balance = self._balances.get(user_id, 0)
        if balance < amount:
            raise ValueError(f"Insufficient credits: have {balance}, need {amount}")
        self._balances[user_id] = balance - amount
        freeze_id = f"freeze-{self._counter}"
        self._counter += 1
        self._freezes[freeze_id] = {
            "user_id": user_id, "amount": amount,
            "reason": reason, "status": "frozen",
        }
        return freeze_id

    def commit(self, freeze_id: str) -> bool:
        freeze = self._freezes.get(freeze_id)
        if freeze is None or freeze["status"] != "frozen":
            return False
        freeze["status"] = "committed"
        return True

    def unfreeze(self, freeze_id: str) -> bool:
        freeze = self._freezes.get(freeze_id)
        if freeze is None or freeze["status"] != "frozen":
            return False
        freeze["status"] = "unfrozen"
        self._balances[freeze["user_id"]] += freeze["amount"]
        return True

    def get_balance(self, user_id: str) -> int:
        return self._balances.get(user_id, 0)


# ---------------------------------------------------------------------------
# Mock implementations (async)
# ---------------------------------------------------------------------------


class AsyncMockPaymentService(AsyncPaymentService):
    """Async in-memory mock. Wraps :class:`MockPaymentService` with coroutines."""

    def __init__(self) -> None:
        self._sync = MockPaymentService()

    @property
    def fail_next_charge(self) -> bool:
        return self._sync.fail_next_charge

    @fail_next_charge.setter
    def fail_next_charge(self, value: bool) -> None:
        self._sync.fail_next_charge = value

    async def charge(self, order_id: Any, amount: int, currency: str = "CNY") -> str:
        return self._sync.charge(order_id, amount, currency)

    async def refund(self, transaction_id: str) -> bool:
        return self._sync.refund(transaction_id)

    async def get_status(self, transaction_id: str) -> str:
        return self._sync.get_status(transaction_id)


class AsyncMockCreditService(AsyncCreditService):
    """Async in-memory mock. Wraps :class:`MockCreditService` with coroutines."""

    def __init__(self) -> None:
        self._sync = MockCreditService()

    def set_balance(self, user_id: str, amount: int) -> None:
        self._sync.set_balance(user_id, amount)

    async def freeze(self, user_id: str, amount: int, reason: str = "") -> str:
        return self._sync.freeze(user_id, amount, reason)

    async def commit(self, freeze_id: str) -> bool:
        return self._sync.commit(freeze_id)

    async def unfreeze(self, freeze_id: str) -> bool:
        return self._sync.unfreeze(freeze_id)

    async def get_balance(self, user_id: str) -> int:
        return self._sync.get_balance(user_id)


__all__ = [
    "AsyncCreditService",
    "AsyncMockCreditService",
    "AsyncMockPaymentService",
    "AsyncPaymentService",
    "CreditService",
    "MockCreditService",
    "MockPaymentService",
    "PaymentService",
]
