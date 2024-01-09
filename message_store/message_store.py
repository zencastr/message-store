import asyncio
import json
from typing import Optional, Dict, Callable

from nats.aio.client import Client
from nats.js.api import PubAck
import nats.js.errors

from .message import Message
from .projections.fetch import Fetch
from .projections.projection import Projection
from .message_from_subscription import MessageFromSubscription
from .subscriptions.subscription import Subscription
from .message_store_logger import message_store_logger
from .timeout_exception import TimeoutException


class MessageStore:
    def __init__(
        self, nats_connection: Client, prefix: str, should_create_missing_streams=False
    ):
        if prefix.endswith("."):
            prefix = prefix[:-1]
        self._nats_connection = nats_connection
        self._jetstream = nats_connection.jetstream()
        self._should_create_missing_streams = should_create_missing_streams
        self._nats_subject_prefix = f"{prefix}." if prefix != "" else ""
        self._nats_stream_prefix = f"{prefix}-" if prefix != "" else ""

    async def ensure_stream(self, category_name: str) -> None:
        """
        Will create a stream with {prefix}.category_name if the constructor was
        called with should_create_missing_streams=True (default is False)
        Otherwise an exception will be raised
        The term category comes from here: http://docs.eventide-project.org/user-guide/stream-names/#parts
        """
        nats_stream_subject = f"{self._nats_subject_prefix}{category_name}.>"
        try:
            stream_name = await self._jetstream.find_stream_name_by_subject(
                nats_stream_subject
            )
            message_store_logger.info(
                f"Stream covering subject {nats_stream_subject} exists. Its name is {stream_name}"
            )

        except nats.js.errors.NotFoundError:
            new_stream_name = f"{self._nats_stream_prefix}{category_name}"
            if self._should_create_missing_streams:
                message_store_logger.info(
                    f"Stream covering subject {nats_stream_subject} does not exist, creating one named {new_stream_name}"
                )
                await self._jetstream.add_stream(
                    name=new_stream_name, subjects=[nats_stream_subject]
                )
                message_store_logger.info(
                    f"Stream {new_stream_name} created successfuly"
                )
            else:
                raise Exception(
                    f"Stream covering subject {nats_stream_subject} does not exist, please create one named {new_stream_name}"
                ) from None

    async def publish_message(
        self, subject: str, message: Message, msg_id: Optional[str] = None
    ) -> PubAck:
        """
        Publishes a message with the format: type, data and optional metadata to
        the subject (automatically prefixed by the prefix provided to the ctor)
        Returns PubAck that contains:
        duplicate - was there a message published with the same msg_id inside the stream's duplicate window check
        seq - sequence number for the stream
        stream - stream name
        """
        headers: Optional[Dict] = None
        if msg_id is not None:
            headers = {"Nats-Msg-Id": msg_id}
        return await self._jetstream.publish(
            f"{self._nats_subject_prefix}{subject}",
            json.dumps(message.to_dict()).encode("utf8"),
            headers=headers,
        )

    async def fetch(self, subject: str, projection: Projection):
        fetcher = Fetch(self._jetstream, self._nats_subject_prefix)
        return await fetcher.fetch(subject, projection)

    def create_subscription(
        self,
        subject: str,
        consumer_name: str,
        handlers: dict[str, Callable[[MessageFromSubscription], None]],
        max_number_of_retries: int = 3,
        dead_letter_subject: Optional[str] = None,
    ) -> Subscription:
        return Subscription(
            self._nats_connection,
            self._jetstream,
            self._nats_subject_prefix,
            subject,
            consumer_name,
            handlers,
            max_number_of_retries,
            dead_letter_subject=f"{self._nats_subject_prefix}{dead_letter_subject}"
            if dead_letter_subject is not None
            else None,
        )

    async def wait_for(
        self, subject: str, predicate: Callable[[Message], bool], timeout: int = 5
    ) -> Message:
        """
        Waits for a message (event/command) on the subject (automatically prefixed by the prefix provided to the ctor)
        that matches the predicate. Returns the message if found, otherwise raises TimeoutException
        """
        subscription = await self._nats_connection.subscribe(
            f"{self._nats_subject_prefix}{subject}"
        )

        async def start_timeout_countdown():
            await asyncio.sleep(timeout)
            try:
                await subscription.unsubscribe()
            except Exception:
                pass

        timeout_task = asyncio.create_task(start_timeout_countdown())

        async for msg in subscription.messages:
            message = Message.create_from_dict(json.loads(msg.data.decode("utf-8")))
            if predicate(message):
                timeout_task.cancel()
                try:
                    await subscription.unsubscribe()
                except Exception:
                    pass
                return message

        raise TimeoutException(f"Timed out waiting for a message on subject {subject}")
