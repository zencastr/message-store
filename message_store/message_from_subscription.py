from typing import Optional, Dict, Any
from .message_metadata import MessageMetadata
from nats.aio.msg import Msg
import json


class MessageFromSubscription:
    def __init__(
        self,
        type: str,
        data: Dict[str, Any],
        seq: int,
        subject: str,
        metadata: Optional[MessageMetadata] = None,
        is_last_attempt: Optional[bool] = None,
    ):
        self.type = type
        self.data = data
        self.seq = seq
        self.subject = subject
        self.is_last_attempt = is_last_attempt
        self.metadata = metadata
        self._terminate_flag = False

    def to_dict(self):
        result = {
            "type": self.type,
            "data": self.data,
            "subject": self.subject,
            "seq": self.seq,
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
            metadata=MessageMetadata.create_from_dict(parsed_message_data["metadata"])
            if "metadata" in parsed_message_data
            else None,
            is_last_attempt=message.metadata.num_delivered >= max_number_of_redeliveries
            if max_number_of_redeliveries is not None
            else None,  # it might actually go over the max_number_of_redelivereis because of timeouts
        )

    def __repr__(self):
        return str(self.to_dict())
