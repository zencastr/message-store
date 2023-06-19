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
        if self.metadata != None:
            result["metadata"] = self.metadata.to_dict()
        return result
