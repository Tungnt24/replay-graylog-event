import json
import datetime

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition, OffsetAndMetadata

from replay_graylog_event.utils.logger import logger
from replay_graylog_event.setting import KafkaConfig


class KafkaClient:
    def __init__(self) -> None:
        self.consumer = None
        self.producer = None
        self.kafka_broker = KafkaConfig.kafka_broker
        self.kafka_consumer_topic = KafkaConfig.kafka_consumer_topic
        self.kafka_producer_topic = KafkaConfig.kafka_producer_topic
        self.kafka_group_id = KafkaConfig.kafka_group_id
        self.auto_offset_reset = KafkaConfig.kafka_auto_offset_reset
        self.value_deserializer = lambda x: json.loads(
            x.decode("utf-8", "ignore")
        )
        self.value_serializer = lambda x: json.dumps(x).encode("utf-8")
        self.enable_auto_commit = KafkaConfig.kafka_enable_auto_commit
        self.max_poll_records = KafkaConfig.kafka_max_poll_records
        self.poll_timeout = KafkaConfig.kafka_poll_timeout

    def create_consumer(self):
        self.consumer = KafkaConsumer(
            self.kafka_consumer_topic,
            bootstrap_servers=self.kafka_broker,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=self.value_deserializer,
            enable_auto_commit=self.enable_auto_commit,
            max_poll_records=self.max_poll_records,
        )

    def create_producer(self):
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=self.value_serializer,
        )

    def send_message(self, user, event, offset):
        logger.info(f"SENDING EVENT: {event} | USER: {user}")
        self.producer.send(
            topic=self.kafka_producer_topic,
            key=bytes(user, "utf-8"),
            value=event,
            partition=0,
        )
        self.producer.flush()

    def current_possion(self, partition):
        tp = TopicPartition(self.kafka_consumer_topic, partition)
        return self.consumer.position(tp)

    def poll_message(self):
        msg = self.consumer.poll(self.poll_timeout)
        return msg

    def assign_partition(self, partition):
        tp = TopicPartition(self.kafka_consumer_topic, partition)
        self.consumer.assign([tp])

    def seek_message(self, partition=0, offset_start=0):
        tp = TopicPartition(self.kafka_consumer_topic, partition)
        self.consumer.seek(tp, offset_start)
        return self.consumer

    def get_offset_and_timestamp(self, tp, timestamp_start, timestamp_end):
        offset_and_timestamp_start = self.consumer.offsets_for_times(
            {tp: int(timestamp_start)}
        )
        offset_and_timestamp_end = self.consumer.offsets_for_times(
            {tp: int(timestamp_end)}
        )
        offset_and_timestamp_start = list(offset_and_timestamp_start.values())[
            0
        ]
        offset_and_timestamp_end = list(offset_and_timestamp_end.values())[0]
        if (
            offset_and_timestamp_start is None
            or offset_and_timestamp_end is None
        ):
            return None, None
        return offset_and_timestamp_start, offset_and_timestamp_end

    def get_offset(self, partition, timestamp_start, timestamp_end):
        tp = TopicPartition(self.kafka_consumer_topic, partition)
        (
            offset_timestamp_start,
            offset_timestamp_end,
        ) = self.get_offset_and_timestamp(tp, timestamp_start, timestamp_end)
        if offset_timestamp_start is None or offset_timestamp_start is None:
            logger.error("could not found offset and timestamp")
            offset_start, offset_end = 0, 0
        else:
            offset_start = offset_timestamp_start.offset
            offset_end = offset_timestamp_end.offset
        return offset_start, offset_end

    def convert_to_timestamp(self, datetime_str: str):
        time_format = datetime_str[-2:]
        local_time = datetime.datetime.strptime(
            datetime_str, "%m/%d/%Y %H:%M:%S"
        )
        if time_format == "PM":
            new_datetime = datetime_str.replace(time_format, "")
            local_time = datetime.datetime.strptime(
                new_datetime, "%m/%d/%Y %H:%M:%S"
            )
            hours_added = datetime.timedelta(hours=12)
            if local_time.hour != 12:
                local_time = local_time + hours_added
        elif time_format == "AM" and local_time.hour == 12:
            new_datetime = datetime_str.replace(time_format, "")
            local_time = datetime.datetime.strptime(
                new_datetime, "%m/%d/%Y %H:%M:%S"
            )
            hours_added = datetime.timedelta(hours=12)
            local_time = local_time - hours_added
        logger.info(f"LOCAL TIME: {local_time}")
        timestamp = int(datetime.datetime.timestamp(local_time))
        return timestamp

    def handle_timestamp(
        self,
        start: str,
        end: str,
    ):
        start = start.replace(",", " ")
        end = end.replace(",", " ")
        timestamp_start, timestamp_end = [
            self.convert_to_timestamp(time) for time in [start, end]
        ]
        return timestamp_start * 1000, timestamp_end * 1000
