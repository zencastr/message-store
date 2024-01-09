from typing import Optional, Dict, Any


class MessageMetadata:
    originSubject: Optional[str]
    traceId: Optional[str]
    additional_props: Dict[str, Any]

    def __init__(
        self,
        origin_subject: Optional[str] = None,
        trace_id: Optional[str] = None,
        **additional_props: Any
    ) -> None:
        self.originSubject = origin_subject
        self.traceId = trace_id
        self.additional_props = additional_props

    @staticmethod
    def create_from_dict(metadata_dictionary: Dict[str, Any]):
        origin_subject = None
        trace_id = None
        if "originSubject" in metadata_dictionary:
            origin_subject = metadata_dictionary["originSubject"]
            del metadata_dictionary["originSubject"]
        if "traceId" in metadata_dictionary:
            trace_id = metadata_dictionary["traceId"]
            del metadata_dictionary["traceId"]

        return MessageMetadata(origin_subject, trace_id, **metadata_dictionary)

    def to_dict(self):
        result = {}
        if len(self.additional_props) != 0:
            result = self.additional_props
        if self.originSubject is not None:
            result["originSubject"] = self.originSubject
        if self.traceId is not None:
            result["traceId"] = self.traceId
        return result
