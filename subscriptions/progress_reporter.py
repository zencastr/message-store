import asyncio
from typing import Optional, Callable
from nats.aio.msg import Msg
import logging


class ProgressReporter:
    """
    Sends the +WPI to nats jetstream to indicate the message
    is still being worked on. The default AckWait window is 30 secs
    The default  for the progress reporter 15 secs
    """

    def __init__(self, report_interval_in_seconds=15):
        self.__reportIntervalInSeconds = report_interval_in_seconds
        self.__progressTask: Optional[asyncio.Task[None]]
        self.__progressTask = None

    def start_reporting_progress(self, jetstream_message: Msg):
        self.__progressTask = asyncio.create_task(
            self.__report_progress(jetstream_message)
        )

    async def __report_progress(self, jetstream_message: Msg):
        await asyncio.sleep(self.__reportIntervalInSeconds)
        await jetstream_message.in_progress()
        logging.debug(
            f"Sent +WPI to jetstream for message with seq: {jetstream_message.metadata.sequence.stream}, subject {jetstream_message.subject} from stream {jetstream_message.metadata.stream}"
        )
        await self.__report_progress(jetstream_message)

    def stop_reporting_progress(self):
        if self.__progressTask != None:
            self.__progressTask.cancel()
            self.__progressTask = None
