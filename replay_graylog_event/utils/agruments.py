import argparse

parser = argparse.ArgumentParser()
arg = parser.add_mutually_exclusive_group()
arg.add_argument(
    "-rwt",
    "--replay_with_timestamp",
    type=str,
    nargs="+",
    default=[],
    help="python3 replay_graylog_event/run.py -rwt time_start time_end",
)
args = parser.parse_args()
