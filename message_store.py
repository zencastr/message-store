from ast import Dict
import logging
from typing import Optional
from nats.aio.client import Client
from .message import Message
from nats.js.api import PubAck
import json


class MessageStore:
    def __init__(
        self, nats_connection: Client, prefix: str, should_create_missing_streams=False
    ):
        self.__jetstream = nats_connection.jetstream()
        self.__should_create_missing_streams = should_create_missing_streams
        self.__nats_subject_prefix = f"{prefix}." if prefix != "" else ""
        self.__nats_stream_prefix = f"{prefix}-" if prefix != "" else ""

    async def ensure_stream(self, category_name: str) -> None:
        """
        Will create a stream with {prefix}.category_name if the constructor was
        called with should_create_missing_streams=True (default is False)
        Otherwise an exception will be raised
        The term category comes from here: http://docs.eventide-project.org/user-guide/stream-names/#parts
        """
        nats_stream_subject = f"{self.__nats_subject_prefix}{category_name}.>"
        try:
            stream_name = await self.__jetstream.find_stream_name_by_subject(
                nats_stream_subject
            )
            logging.info(
                f"Stream covering subject {nats_stream_subject} exists. Its name is {stream_name}"
            )

        except:
            new_stream_name = f"{self.__nats_stream_prefix}{category_name}"
            if self.__should_create_missing_streams:
                logging.info(
                    f"Stream covering subject {nats_stream_subject} does not exist, creating one named {new_stream_name}"
                )
                await self.__jetstream.add_stream(
                    name=new_stream_name, subjects=[nats_stream_subject]
                )
                logging.info(f"Stream {new_stream_name} created successfuly")
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
        if msg_id != None:
            headers["Nats-Msg-Id"] = msg_id
        return await self.__jetstream.publish(
            f"{self.__nats_subject_prefix}{subject}",
            json.dumps(message.to_dict()).encode("utf8"),
            headers=headers,
        )
