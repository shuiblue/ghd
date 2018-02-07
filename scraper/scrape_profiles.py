#!/usr/bin/env python

import pandas as pd

import argparse

import github


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Scrape GitHub profile information for specified logins. "
                    "List of logins is accepted from input (each login on a "
                    "new line), profiles are output as a CSV file with a "
                    "header row.")
    parser.add_argument('token', nargs="+",
                        help='a GitHub API token')
    parser.add_argument('-i', '--input', default="-", nargs="?",
                        type=argparse.FileType('r'),
                        help='File to use as input, empty or "-" for stdin')
    parser.add_argument('-o', '--output', default="-",
                        type=argparse.FileType('w'),
                        help='Output filename, "-" or skip for stdout')
    args = parser.parse_args()

    tokens = args.token or None
    api = github.GitHubAPI(tokens=tokens)


    def gen():
        for login in args.input:
            try:
                yield api.user_info(login.strip())
            except github.RepoDoesNotExist:
                continue

    pd.DataFrame(gen()).to_csv(args.output, encoding="utf8")
