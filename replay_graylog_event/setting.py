from environs import Env

env = Env()
env.read_env()


class KafkaConfig:
    kafka_broker = env.list("KAFKA_BROKER", [])
    kafka_consumer_topic = env.str("KAFKA_CONSUMER_TOPIC", "")
    kafka_producer_topic = env.str("KAFKA_PRODUCER_TOPIC", "")
    kafka_group_id = env.str("KAFKA_GROUP_ID", "")
    kafka_auto_offset_reset = env.str("KAFKA_AUTO_OFFSET_RESET", "")
    kafka_enable_auto_commit = env.bool("KAFKA_ENABLE_AUTO_COMMIT", False)
    kafka_max_poll_records = env.int("KAFKA_MAX_POLL_RECORS")
    kafka_poll_timeout = env.int("KAFKA_POLL_TIMEOUT")
