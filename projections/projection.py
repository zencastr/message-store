from typing import TypeVar, Callable, Any
from ..message_from_subscription import MessageFromSubscription

T = TypeVar("T")


class Projection:
    def __init__(
        self,
        init: Callable[[None], T],
        handlers: dict[str, Callable[[T, MessageFromSubscription], T]],
    ):
        self.handlers = handlers
        self._entity = init()

    def handle(self, type: str, message: MessageFromSubscription):
        if type in self.handlers:
            self._entity = self.handlers[type](self._entity, message)

    def get_result(self) -> T:
        return self._entity
