import argparse


# Parsing arguments
def cli_parser(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "option",
        help=""" Choose an option on how to run this program. Possible
                        choices include 'instances', 'backup', 'clean'.""",
    )
    return parser.parse_args(args)
