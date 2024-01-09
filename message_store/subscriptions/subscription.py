from nats.errors import TimeoutError, ConnectionClosedError
from nats.aio.client import Client as NatsClient
from nats.js.client import JetStreamContext
from nats.aio.msg import Msg
from typing import Dict, Callable, Optional
from ..message_from_subscription import MessageFromSubscription
from .progress_reporter import ProgressReporter
import asyncio
from ..message_store_logger import message_store_logger
import json


class Subscription:
    def __init__(
        self,
        nats_connection: NatsClient,
        jetstream_client: JetStreamContext,
        nats_subject_prefix: str,
        subject: str,
        consumer_name: str,
        handlers: Dict[str, Callable[[MessageFromSubscription], None]],
        max_number_of_retries: Optional[int] = 3,
        dead_letter_subject: Optional[str] = None,
    ):
        self._nats_connection = nats_connection
        self._jetstream_client = jetstream_client
        self._nats_subject_prefix = nats_subject_prefix
        self._subject = subject
        self._handlers = handlers
        self._consumer_name = consumer_name
        self._is_subscription_active = False
        self._pull_wait_timeout_in_secs = 5
        self._max_number_of_retries = max_number_of_retries
        self._dead_letter_subject = dead_letter_subject
        self._running_subscription_task: Optional[asyncio.Task]
        self._running_subscription_task = None

    def start(self) -> asyncio.Task:
        self._is_subscription_active = True

        async def start_pull_subscription():
            pull_subscription = await self._jetstream_client.pull_subscribe(
                f"{self._nats_subject_prefix}{self._subject}",
                durable=self._consumer_name,
            )
            progress_reporter = ProgressReporter()
            while not self._nats_connection.is_closed and self._is_subscription_active:
                try:
                    jetstream_messages = await pull_subscription.fetch(
                        batch=1, timeout=self._pull_wait_timeout_in_secs
                    )
                    jetstream_message = jetstream_messages[
                        0
                    ]  # if there are no messages then TimeoutError will be raised, if it gets here there's 1 message
                except TimeoutError:
                    message_store_logger.debug(
                        f'No messages arrived during the pull_wait_timeout_in_secs ({self._pull_wait_timeout_in_secs}) for subject {self._nats_subject_prefix}{self._subject}. "Re-arming" wait for messages'
                    )
                    continue
                except ConnectionClosedError:
                    message_store_logger.info(
                        f"Connection to nats was closed, stopping subscription to {self._subject}"
                    )
                    break

                try:
                    if self._was_message_redelivered_too_many_times(jetstream_message):
                        await self._terminate_message(jetstream_message)
                        continue

                    progress_reporter.start_reporting_progress(jetstream_message)

                    message = MessageFromSubscription.create_from_js_message(
                        self._nats_subject_prefix,
                        jetstream_message,
                        self._max_number_of_retries,
                    )
                    if message.type in self._handlers:
                        message_store_logger.debug(
                            f"Calling handler for {message.type}, full message: {json.dumps(message.to_dict(), indent=2)}",
                        )
                        if asyncio.iscoroutinefunction(self._handlers[message.type]):
                            await self._handlers[message.type](message)
                        else:
                            self._handlers[message.type](message)
                    else:
                        message_store_logger.debug(
                            f"Ignoring message. Could not find a handler for message with type {message.type}, subject: {jetstream_message.subject}, stream: {jetstream_message.metadata.stream}. Full message:"\
                            f"{json.dumps(message.to_dict(), indent=2)}",
                        )
                    if message.is_marked_for_termination():
                        await self._terminate_message(jetstream_message)
                    else:
                        await jetstream_message.ack()
                except ConnectionClosedError:
                    message_store_logger.warning(
                        f"Connection to nats/jetstream was closed while handling {message}. It will be retried if it wasn't the last attempt (is_last_attempt != False). Stopping subscription to {self._subject}"
                    )
                    break
                except (Exception, asyncio.CancelledError) as exception:
                    message_store_logger.warning(
                        f"Failed to handle message with subject {jetstream_message.subject}, seq: {jetstream_message.metadata.sequence.stream}, data: {jetstream_message.data}, exception: {type(exception).__name__} {exception}"
                    )
                    if not self._nats_connection.is_closed:
                        if message.is_marked_for_termination():
                            await self._terminate_message(jetstream_message)
                        else:
                            await jetstream_message.nak()
                finally:
                    progress_reporter.stop_reporting_progress()

        self._running_subscription_task = asyncio.create_task(start_pull_subscription())
        return self._running_subscription_task

    async def stop(self):
        self._is_subscription_active = False
        if self._running_subscription_task:
            await self._running_subscription_task
            self._running_subscription_task = None

    def _was_message_redelivered_too_many_times(self, message: Msg):
        if self._max_number_of_retries is None:
            return False
        return message.metadata.num_delivered > self._max_number_of_retries

    async def _terminate_message(self, message: Msg):
        await message.term()
        overdelivery_warning = f" This attempt (#{message.metadata.num_delivered}) exceeds max of {self._max_number_of_retries}" if self._was_message_redelivered_too_many_times(message) else ""
        message_store_logger.warning(
            f"Giving up on processing message #{message.metadata.sequence.stream}, subject {message.subject} from stream {message.metadata.stream}." + overdelivery_warning
        )
        if self._dead_letter_subject is not None:
            failed_message_subject_without_prefix = message.subject[
                len(self._nats_subject_prefix) :
            ]
            dead_letter_subject_for_failed_msg = (
                f"{self._dead_letter_subject}.{failed_message_subject_without_prefix}"
            )
            message_store_logger.info(
                f"Sending #{message.metadata.sequence.stream}, subject {message.subject} from stream {message.metadata.stream} to dead letter with subject ({dead_letter_subject_for_failed_msg})"
            )
            await self._jetstream_client.publish(
                dead_letter_subject_for_failed_msg,
                message.data,
            )
