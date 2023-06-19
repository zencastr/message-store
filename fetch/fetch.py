from nats.js.client import JetStreamContext
from nats.js.api import ConsumerInfo
from .projection import Projection
from ..message_from_subscription import MessageFromSubscription


class Fetch:
    def __init__(self, jetstream_client: JetStreamContext, nats_subject_prefix: str):
        self.__jetstream_client = jetstream_client
        self.__nats_subject_prefix = nats_subject_prefix

    def __get_total_number_of_messages_in_consumer(self, consumer_info: ConsumerInfo):
        return consumer_info.num_pending + consumer_info.delivered.consumer_seq

    def __has_consumer_any_messages(self, consumer_info: ConsumerInfo):
        return self.__get_total_number_of_messages_in_consumer(consumer_info) > 0

    async def fetch(self, subject: str, projection: Projection):
        subscription = await self.__jetstream_client.subscribe(
            f"{self.__nats_subject_prefix}{subject}", ordered_consumer=True
        )
        consumer_info = await subscription.consumer_info()
        if not self.__has_consumer_any_messages(consumer_info):
            return projection.get_result()

        total_messages_in_stream = self.__get_total_number_of_messages_in_consumer(
            consumer_info
        )
        processed_count = 0
        async for jetstream_message in subscription.messages:
            message = MessageFromSubscription.create_from_js_message(
                self.__nats_subject_prefix, jetstream_message
            )
            projection.handle(message.type, message)
            processed_count += 1
            await jetstream_message.ack()
            if processed_count == total_messages_in_stream:
                await subscription.unsubscribe()
                break

        return projection.get_result()
