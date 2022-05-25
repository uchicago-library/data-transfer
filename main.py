import argparse
import configparser
import logging
import sys

from folioclient import FolioClient


def error_exit(status, msg):
    sys.stderr.write(msg)
    sys.exit(status)


def read_config(filename: str):
    """Parse the named config file and return an config object"""

    config = configparser.ConfigParser()
    try:
        config.read_file(open(filename))
    except FileNotFoundError as err:
        msg = f"{type(err).__name__}: {err}\n"
        error_exit(1, msg)
    except configparser.MissingSectionHeaderError as err:
        msg = f"{type(err).__name__}: {err}\n"
        error_exit(2, msg)
    return config


def parse_args():
    """Parse command line arguments and return a Namespace object."""

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-i",
        "--infile",
        help="Input file (default: stdin)",
        default=sys.stdin,
        type=argparse.FileType("r"),
    )
    parser.add_argument(
        "-o",
        "--outfile",
        help="Output file (truncate if exists, default: stdout)",
        default=sys.stdout,
        type=argparse.FileType("w"),
    )
    parser.add_argument(
        "-C", "--config_file", help="Name of config file", default="config.ini"
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase verbosity level"
    )
    return parser.parse_args()


def parse_data(line):
    """Placeholder function for parsing input data"""
    return line


def process_data(client, data):
    """Placeholder for processing the data"""
    return data


def write_result(out, output):
    """Placeholder for writing output"""
    out.write(output)


def main_loop(client, infile, outfile):
    for line in infile:
        data = parse_data(line)
        result = process_data(client, data)
        write_result(outfile, result)


def main():
    args = parse_args()
    config = read_config(args.config_file)
    # Logic or function to override config values from the command line arguments would go here

    client = FolioClient(
        config["Okapi"]["okapi_url"],
        config["Okapi"]["tenant_id"],
        config["Okapi"]["username"],
        config["Okapi"]["password"],
    )

    main_loop(client, args.infile, args.outfile)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Interrupted")
        sys.exit(0)
