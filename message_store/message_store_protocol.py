from typing import Callable, Dict, Optional, Protocol

from nats.js.api import PubAck

from .message import Message
from .message_from_subscription import MessageFromSubscription
from .projections.projection import Projection
from .subscriptions.subscription import Subscription


class MessageStoreProtocol(Protocol):
    """
    Structural interface for the message store's public surface.

    The concrete ``MessageStore`` satisfies it, and so do wrappers (e.g. the OpenTelemetry
    interceptor) that delegate to a store. Type code against this protocol when you want to accept
    either the concrete store or a wrapped one.

    This is a static-typing contract only; it is intentionally not ``@runtime_checkable``. An
    ``isinstance`` check against a runtime-checkable protocol relies on ``inspect.getattr_static``,
    which cannot see methods a wrapper exposes via ``__getattr__`` delegation — so it would wrongly
    report such a wrapper as not conforming. Use the protocol for annotations, not ``isinstance``.
    """

    async def ensure_stream(
        self,
        category_name: str,
        max_bytes_on_create: int = 2**30,
        max_msg_size_on_create: int = 2**22,
    ) -> None: ...

    async def publish_message(
        self,
        subject: str,
        message: Message,
        msg_id: Optional[str] = None,
        timeout_in_seconds: Optional[float] = 60,
        headers: Optional[Dict[str, str]] = None,
    ) -> PubAck: ...

    def create_subscription(
        self,
        subject: str,
        consumer_name: str,
        handlers: Dict[str, Callable[[MessageFromSubscription], None]],
        max_number_of_retries: int = 3,
        dead_letter_subject: Optional[str] = None,
    ) -> Subscription: ...

    async def fetch(self, subject: str, projection: Projection): ...

    async def wait_for(
        self, subject: str, predicate: Callable[[Message], bool], timeout: int = 5
    ) -> Message: ...
