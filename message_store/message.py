from typing import Optional, Dict, Any
from .message_metadata import MessageMetadata


class Message:
    def __init__(
        self,
        type: str,
        data: Dict[str, Any],
        metadata: Optional[MessageMetadata] = None,
    ):
        self.type = type
        self.data = data
        self.metadata = metadata

    def to_dict(self):
        result = {
            "type": self.type,
            "data": self.data,
        }
        if self.metadata is not None:
            result["metadata"] = self.metadata.to_dict()
        return result

    @staticmethod
    def create_from_dict(message_dict: Dict[str, Any]):
        return Message(
            type=message_dict["type"],
            data=message_dict["data"],
            metadata=MessageMetadata.create_from_dict(message_dict["metadata"])
            if "metadata" in message_dict
            else None,
        )

    def __repr__(self):
        return str(self.to_dict())
