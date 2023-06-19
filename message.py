from typing import Optional, Dict, Any


class MessageMetadata:
    originSubject: Optional[str]
    traceId: Optional[str]
    additionalProps: Dict[str, Any]

    def __init__(
        self,
        origin_subject: Optional[str] = None,
        trace_id: Optional[str] = None,
        **additional_props: Any
    ) -> None:
        self.origin_subject = origin_subject
        self.trace_id = trace_id
        self.additional_props = additional_props

    def to_dict(self):
        result = {}
        if len(self.additional_props) != 0:
            result = self.additional_props
        if self.origin_subject != None:
            result["originSubject"] = self.origin_subject
        if self.trace_id != None:
            result["traceId"] = self.trace_id
        return result


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