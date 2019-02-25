from dramatiq.encoder import Encoder, MessageData
import json


class BANZAIEncoder(Encoder):
    """Encodes messages as JSON.  This is the default encoder.
    """

    def encode(self, data: MessageData) -> bytes:
        return json.dumps(data, separators=(",", ":")).encode("utf-8")

    def decode(self, data: bytes) -> MessageData:
        return json.loads(data.decode("utf-8"))
