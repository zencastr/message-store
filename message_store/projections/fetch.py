from nats.js.client import JetStreamContext
from nats.js.api import ConsumerInfo
from .projection import Projection
from ..message_from_subscription import MessageFromSubscription


class Fetch:
    def __init__(self, jetstream_client: JetStreamContext, nats_subject_prefix: str):
        self._jetstream_client = jetstream_client
        self._nats_subject_prefix = nats_subject_prefix

    async def fetch(self, subject: str, projection: Projection):
        subscription = await self._jetstream_client.subscribe(
            f"{self._nats_subject_prefix}{subject}", ordered_consumer=True
        )
        consumer_info = await subscription.consumer_info()
        if not self._has_consumer_any_messages(consumer_info):
            return projection.get_result()

        total_messages_in_stream = self._get_total_number_of_messages_in_consumer(
            consumer_info
        )
        processed_count = 0
        async for jetstream_message in subscription.messages:
            message = MessageFromSubscription.create_from_js_message(
                self._nats_subject_prefix, jetstream_message
            )
            projection.handle(message.type, message)
            processed_count += 1
            if processed_count == total_messages_in_stream:                                
                await subscription.unsubscribe()
                await self.ensure_consumer_is_deleted(subject, consumer_name=consumer_info.name)                


        return projection.get_result()

    def _get_total_number_of_messages_in_consumer(self, consumer_info: ConsumerInfo):
        return consumer_info.num_pending or 0 + consumer_info.delivered.consumer_seq if consumer_info.delivered else 0

    def _has_consumer_any_messages(self, consumer_info: ConsumerInfo):
        return self._get_total_number_of_messages_in_consumer(consumer_info) > 0


    async def ensure_consumer_is_deleted(self, subject: str, consumer_name: str) -> None:
        """
        Jetstream (at least synadia) sometimes takes its time to delete the consumer even when it's ephemeral
        This method will try to actively delete the consumer
        """        
        try:            
            stream = await self._jetstream_client.find_stream_name_by_subject(subject=f"{self._nats_subject_prefix}{subject}")            
            await self._jetstream_client.delete_consumer(stream, consumer_name)            
        except Exception as e:            
            pass

        