from nats.errors import TimeoutError
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
        jetstream_client: JetStreamContext,
        nats_subject_prefix: str,
        subject: str,
        consumer_name: str,
        handlers: Dict[str, Callable[[MessageFromSubscription], None]],
        max_number_of_retries: Optional[int] = 3,
        dead_letter_subject: Optional[str] = None,
    ):
        self.__jetstream_client = jetstream_client
        self.__nats_subject_prefix = nats_subject_prefix
        self.__subject = subject
        self.__handlers = handlers
        self.__consumer_name = consumer_name
        self.__is_subscription_active = False
        self.__pull_wait_timeout_in_secs = 5
        self.__max_number_of_retries = max_number_of_retries
        self.__dead_letter_subject = dead_letter_subject
        self.__running_subscription_task: Optional[asyncio.Task]
        self.__running_subscription_task = None

    def start(self) -> asyncio.Task:
        self.__is_subscription_active = True

        async def start_pull_subscription():
            pull_subscription = await self.__jetstream_client.pull_subscribe(
                f"{self.__nats_subject_prefix}{self.__subject}",
                durable=self.__consumer_name,
            )
            progress_reporter = ProgressReporter()
            while self.__is_subscription_active:
                try:
                    jetstream_messages = await pull_subscription.fetch(
                        batch=1, timeout=self.__pull_wait_timeout_in_secs
                    )
                    jetstream_message = jetstream_messages[
                        0
                    ]  # if there are no messages then TimeoutError will be raised, if it gets here there's 1 message
                except TimeoutError:
                    message_store_logger.debug(
                        f'No messages arrived during the pull_wait_timeout_in_secs ({self.__pull_wait_timeout_in_secs}) for subject {self.__nats_subject_prefix}{self.__subject}. "Re-arming" wait for messages'
                    )
                    continue

                try:
                    if self._was_message_redelivered_too_many_times(jetstream_message):
                        await self._terminate_message(jetstream_message)
                        continue

                    progress_reporter.start_reporting_progress(jetstream_message)

                    message = MessageFromSubscription.create_from_js_message(
                        self.__nats_subject_prefix,
                        jetstream_message,
                        self.__max_number_of_retries,
                    )
                    if message.type in self.__handlers:
                        message_store_logger.debug(
                            f"Calling handler for {message.type}, full message: {json.dumps(message.to_dict(), indent=2)}",
                        )
                        if asyncio.iscoroutinefunction(self.__handlers[message.type]):
                            await self.__handlers[message.type](message)
                        else:
                            self.__handlers[message.type](message)
                    else:
                        message_store_logger.debug(
                            f"Ignoring message. Could not find a handler for message with type {message.type}, subject: {jetstream_message.subject}, stream: {jetstream_message.metadata.stream}. Full message:\n",
                            json.dumps(message.to_dict(), indent=2),
                        )
                    await jetstream_message.ack()

                except Exception as exception:
                    message_store_logger.warning(
                        f"Failed to handle with {message.subject}, data: {message.data}, exception: {exception}"
                    )
                    raise
                finally:
                    progress_reporter.stop_reporting_progress()

        self.__running_subscription_task = asyncio.create_task(
            start_pull_subscription()
        )
        return self.__running_subscription_task

    async def stop(self):
        self.__is_subscription_active = False
        if self.__running_subscription_task:
            await self.__running_subscription_task
            self.__running_subscription_task = None

    def _was_message_redelivered_too_many_times(self, message: Msg):
        return message.metadata.num_delivered > self.__max_number_of_retries

    async def _terminate_message(self, message: Msg):
        message_store_logger.warning(
            f"Giving up on processing message #{message.metadata.sequence.stream}, subject {message.subject} from stream {message.metadata.stream}. This attempt (#{message.metadata.num_delivered}) exceeds max of {self.__max_number_of_retries}"
        )
        await message.term()
        if self.__dead_letter_subject != None:
            message_store_logger.info(
                f"Sending #{message.metadata.sequence.stream}, subject {message.subject} from stream {message.metadata.stream} to dead letter with subject ({self.__dead_letter_subject})"
            )
            await self.__jetstream_client.publish(
                self.__dead_letter_subject, message.data
            )
