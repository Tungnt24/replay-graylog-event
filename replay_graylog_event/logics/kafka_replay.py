import json

from replay_graylog_event.logics.client.kafka_client import KafkaClient
from replay_graylog_event.utils.logger import logger


class KafkaReplay:
    def __init__(self) -> None:
        self.kafka_client = KafkaClient()
        self.kafka_client.create_consumer()
        self.kafka_client.create_producer()

    def poll(self, time_start, time_end):
        timestamp_start, timestamp_end = self.kafka_client.handle_timestamp(
            time_start, time_end
        )
        offset_start, offset_end = self.kafka_client.get_offset(
            0, timestamp_start, timestamp_end
        )
        logger.info(f"OFFSET START: {offset_start}")
        logger.info(f"OFFSET END: {offset_end}")
        try:
            possion = self.kafka_client.current_possion(partition=0)
            if possion > 0:
                offset_start = possion
        except AssertionError:
            self.kafka_client.assign_partition(partition=0)

        offset = self.kafka_client.seek_message(offset_start=offset_start)
        for _ in range(offset_start, offset_end):
            msg = next(offset)
            event = msg.value
            message = event.get("message")
            if "kafka_payload" in message:
                message = json.loads(message[message.find("{") :])
                self.send_message(message, msg.offset)

    def send_message(self, message, offset):
        user = message.get("user")
        self.kafka_client.send_message(user, message, offset)
