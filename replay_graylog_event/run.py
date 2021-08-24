from replay_graylog_event.logics.kafka_replay import KafkaReplay
from replay_graylog_event.utils.agruments import args


def main():
    arg = args.__dict__
    with_timestamp = arg.get("replay_with_timestamp")
    time_start = with_timestamp[0]
    time_end = with_timestamp[1]
    replay = KafkaReplay()
    replay.poll(time_start, time_end)


if __name__ == "__main__":
    main()
