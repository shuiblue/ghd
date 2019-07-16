#!/usr/bin/python

import argparse
import pandas as pd

from common import decorators as d
import scraper

import csv

api = scraper.GitHubAPI()


def get_userEmail(loginID):
    # print("get user %s email address " % (loginID))
    return api.userEmail(loginID)


def get_userInfo(loginID):
    print("get user %s info " % (loginID))
    return api.userInfo(loginID)


def get_isFork(repoUrl):
    # print(" check if repo %s a fork" % (repoUrl))
    return api.isFork(repoUrl)


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

    filepath = '/DATA/shurui/ForkData/hardfork/hardforkList.txt'

    with open(filepath) as fp:
        line = fp.readline()
        while line:
            column = line.split(',')
            fork_url = column[0].replace('https://api.github.com/repos/', '')
            print(" check fork %s " % (fork_url))
            upstream_url = column[1].replace('https://api.github.com/repos/', '')

            isFork = get_isFork(fork_url)
            if(isFork =='notExist'):
                print("fork not exist")
                pass
            if (not isFork):
                with open('/DATA/shurui/ForkData/hardfork/NOT_FORK.txt', 'a') as f:
                    print(fork_url, file=f)
                    print(fork_url + ' is not a fork')
                    pass

            fork_loginID = fork_url.split('/')[0]
            upstream_loginID = upstream_url.split('/')[0]

            fork_owner = get_userInfo(fork_loginID)
            upstream_info = get_userInfo(upstream_loginID)

            result = fork_url + ',' + fork_owner + ',' + upstream_info

            with open('/DATA/shurui/ForkData/hardfork/contactInfo.txt', 'a') as f:
                print(result, file=f)
            line = fp.readline()
