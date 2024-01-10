import asyncio
from typing import Optional
from nats.aio.msg import Msg
from ..message_store_logger import message_store_logger


class ProgressReporter:
    """
    Sends the +WPI to nats jetstream to indicate the message
    is still being worked on. The default AckWait window is 30 secs
    The default  for the progress reporter 15 secs
    """

    def __init__(self, report_interval_in_seconds=15):
        self._reportIntervalInSeconds = report_interval_in_seconds
        self._progressTask: Optional[asyncio.Task[None]]
        self._progressTask = None

    def start_reporting_progress(self, jetstream_message: Msg):
        self._progressTask = asyncio.create_task(
            self._report_progress(jetstream_message)
        )

    async def _report_progress(self, jetstream_message: Msg):
        await asyncio.sleep(self._reportIntervalInSeconds)
        await jetstream_message.in_progress()
        message_store_logger.debug(
            f"Sent +WPI to jetstream for message with seq: {jetstream_message.metadata.sequence.stream}, subject {jetstream_message.subject} from stream {jetstream_message.metadata.stream}"
        )
        await self._report_progress(jetstream_message)

    def stop_reporting_progress(self):
        if self._progressTask is not None:
            self._progressTask.cancel()
            self._progressTask = None
