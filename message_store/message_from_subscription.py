from datetime import datetime
import json
from typing import Optional, Dict, Any

from nats.aio.msg import Msg

from .message_metadata import MessageMetadata


class MessageFromSubscription:
    def __init__(
        self,
        type: str,
        data: Dict[str, Any],
        seq: int,
        subject: str,
        timestamp: datetime,
        metadata: Optional[MessageMetadata] = None,
        is_last_attempt: Optional[bool] = None,
        _raw_jetstream_message: Optional[Msg] = None,
    ):
        self.type = type
        self.data = data
        self.seq = seq
        self.subject = subject
        self.is_last_attempt = is_last_attempt
        self.metadata = metadata
        self._terminate_flag = False
        self.timestamp = timestamp
        # The raw underlying JetStream message. Exposed so external concerns (e.g. OpenTelemetry
        # tracing) can read transport-level details such as headers, stream/consumer names,
        # redelivery count and the stored timestamp. Internal by convention (single underscore):
        # the message store retains full ownership of ack/nak/term. Not serialized in to_dict.
        self._raw_jetstream_message = _raw_jetstream_message

    def to_dict(self):
        result = {
            "type": self.type,
            "data": self.data,
            "subject": self.subject,
            "seq": self.seq,
            "timestamp": self.timestamp.timestamp(),
        }
        if self.metadata is not None:
            result["metadata"] = self.metadata.to_dict()
        if self.is_last_attempt is not None:
            result["isLastAttempt"] = self.is_last_attempt
        return result

    def mark_for_termination(self):
        """ Mark message for termination upon handler completion """
        self._terminate_flag = True

    def is_marked_for_termination(self):
        """ Check if message is marked for termination """
        return self._terminate_flag

    @staticmethod
    def create_from_js_message(
        prefix: str, message: Msg, max_number_of_redeliveries: Optional[int] = None
    ):
        parsed_message_data: dict = json.loads(message.data.decode())
        return MessageFromSubscription(
            type=parsed_message_data["type"],
            data=parsed_message_data["data"],
            seq=message.metadata.sequence.stream,
            subject=message.subject[len(prefix) :],
            timestamp=message.metadata.timestamp,
            metadata=MessageMetadata.create_from_dict(parsed_message_data["metadata"])
            if "metadata" in parsed_message_data
            else None,
            is_last_attempt=message.metadata.num_delivered >= max_number_of_redeliveries
            if max_number_of_redeliveries is not None
            else None,  # it might actually go over the max_number_of_redelivereis because of timeouts
            _raw_jetstream_message=message,
        )

    def __repr__(self):
        return str(self.to_dict())
