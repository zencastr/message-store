from typing import Generic, TypeVar, Callable
from ..message_from_subscription import MessageFromSubscription

T = TypeVar("T")


class Projection(Generic[T]):
    def __init__(
        self,
        init: Callable[[], T],
        handlers: dict[str, Callable[[T, MessageFromSubscription], T]],
    ):
        self.handlers = handlers
        self._entity = init()

    def handle(self, type: str, message: MessageFromSubscription):
        if type in self.handlers:
            self._entity = self.handlers[type](self._entity, message)

    def get_result(self) -> T:
        return self._entity
