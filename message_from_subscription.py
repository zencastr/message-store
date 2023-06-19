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

    def to_dict(self):
        result = {
            "type": self.type,
            "data": self.data,
            "subject": self.subject,
            "seq": self.seq,
        }
        if self.metadata != None:
            result["metadata"] = self.metadata.to_dict()
        if self.is_last_attempt != None:
            result["isLastAttempt"] = self.is_last_attempt
        return result

    @staticmethod
    def create_from_js_message(prefix: str, message: Msg):
        parsed_message_data: dict = json.loads(message.data.decode())
        return MessageFromSubscription(
            type=parsed_message_data["type"],
            data=parsed_message_data["data"],
            seq=message.metadata.sequence.stream,
            subject=message.subject[len(prefix) :],
            metadata=MessageMetadata.create_from_dict(parsed_message_data["metadata"])
            if "metadata" in parsed_message_data
            else None,
        )
