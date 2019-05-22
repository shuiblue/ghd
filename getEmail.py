#!/usr/bin/python

import argparse
import pandas as pd

from common import decorators as d
import scraper

import csv

api = scraper.GitHubAPI()


def get_userEmail(loginID):
    print("get user %s email address " % (loginID))
    return api.userEmail(loginID)


def get_repoLastPushDate(repoUrl):
    print("get repo %s last PUSH date " % (repoUrl))
    return api.repoLastPushDate(repoUrl)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="do the magic")
    parser.add_argument('-i', '--input', default="-", nargs="?",
                        type=argparse.FileType('r'),
                        help='File to use as input, empty or "-" for stdin')
    parser.add_argument('-o', '--output', default="-",
                        type=argparse.FileType('w'),
                        help='Output filename, "-" or skip for stdout')
    args = parser.parse_args()

    filepath = '/Users/shuruiz/Work/ForkData/hardfork-exploration/aForkOf-3ForkLevel.csv'
    # filepath = '/home/feature/shuruiz/ForkData/hardfork/aForkOf-3ForkLevel.csv'

    with open(filepath) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for column in csv_reader:
            line_count += 1
            print('line: ' + str(line_count))
            # with open(filepath) as fp:
            #     line = fp.readline()
            #     cnt = 1
            #     while line:

            # column = line.split(',')
            if column[0] == 'fork_url':
                # line = fp.readline()
                continue
            fork_url = column[0].replace('https://api.github.com/repos/', '')
            upstream_url = column[4].replace('https://api.github.com/repos/', '')
            grandPa_url = column[8].replace('https://api.github.com/repos/', '')

            fork_loginID = fork_url.split('/')[0]
            upstream_loginID = upstream_url.split('/')[0]
            grandPa_loginID = grandPa_url.split('/')[0]

            try:
                # get email from github api
                print("fork ---")
                fork_email = get_userEmail(fork_loginID)
                if fork_email is None:
                    print("fork email is null ")
                    fork_email = ''
                # get last update date
                fork_lastPush = get_repoLastPushDate(fork_url)
                if fork_lastPush is None:
                    print("fork_lastPush  is null ")
                    fork_lastPush = ''
                print(fork_email + ' ' + fork_lastPush)

                upstream_email = upstream_lastPush = grandPa_email = grandPa_lastPush = ''

                if upstream_url != '':
                    print("upstream ---")
                    upstream_email = get_userEmail(upstream_loginID)
                    if upstream_email is None:
                        print("upstream email is null ")
                        upstream_email = ''
                    # get last update date
                    upstream_lastPush = get_repoLastPushDate(upstream_url)
                    if upstream_lastPush is None:
                        print("upstream_lastPush  is null ")
                        upstream_lastPush = ''
                    print(upstream_email + ' ' + upstream_lastPush)

                if grandPa_url != '':
                    print("grandPa ---")
                    grandPa_email = get_userEmail(grandPa_loginID)
                    if grandPa_email is None:
                        print("grandPa email is null ")
                        grandPa_email = ''
                    grandPa_lastPush = get_repoLastPushDate(grandPa_url)
                    if grandPa_lastPush is None:
                        print("grandPa_lastPush  is null ")
                        grandPa_lastPush = ''
                    print(grandPa_email + ' ' + grandPa_lastPush)



            except:
                raise

            result = fork_url + ',' + fork_email + ',' + fork_lastPush + ',' + upstream_email + ',' + \
                     upstream_lastPush + ',' + grandPa_email + ',' + grandPa_lastPush

            with open('/Users/shuruiz/Work/ForkData/hardfork-exploration/aForkOf-3ForkLevel_email.csv', 'a') as f:
            # with open('/home/feature/shuruiz/ForkData/hardfork/aForkOf-3ForkLevel_email.csv', 'a') as f:
                print(result, file=f)
            # line = fp.readline()
