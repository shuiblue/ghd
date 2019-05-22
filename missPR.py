#!/usr/bin/python

import argparse
import pandas as pd

from common import decorators as d
import scraper

api = scraper.GitHubAPI()

fs_cache = d.fs_cache('shurui_timeline')


@fs_cache
def get_prs(repo):
    print("get pr from " + repo)
    return pd.DataFrame(api.repo_pulls(repo))


@fs_cache
def get_pr_commits(repo, pr):
    print("get commits of pr %s from %s" % (repo, pr))
    return pd.DataFrame(api.pull_request_commits(repo, pr))


@fs_cache
def get_pr_comments(repo, pr):
    print("get comments of pr %s from %s" % (repo, pr))
    return pd.DataFrame(api.issue_comments(repo, pr))

@fs_cache
def get_pr_timeline(repo, issue):
    print("get reference of pr %s from %s" % (repo, issue))
    return pd.DataFrame(api.issue_pr_timeline(repo, issue))

@fs_cache
def get_pr_changedFiles(repo, issue):
    print("get changed files of pr %s from %s" % (repo, issue))
    return pd.DataFrame(api.issue_pr_timeline(repo, issue))



def RepresentsInt(s):
    print(s)
    if pd.isnull(s):
        print("%s is null", s)
        return False
    try:

        int(s)
        return True
    except ValueError:
        print(s + ", not digit")
        return False


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

    # for repo in args.input:
    #     repo = repo.strip()
    #     # get_pr_timeline('joomla/joomla-cms', 5293)
    #
    #
    #     try:
    #         pullrequests = get_prs(repo)
    #     except:
    #         raise
    #     print(repo)
    #
    #     if len(pullrequests):
    #         for pullrequest_id in pullrequests['id']:
    #             if RepresentsInt(pullrequest_id):
    #                 pullrequest_id = int(pullrequest_id)
    #                 commits = get_pr_commits(repo, pullrequest_id)
    #                 comments = get_pr_comments(repo, pullrequest_id)
    #                 # get_pr_timeline(repo, pullrequest_id)
    #
    #     else:
    #         print(" no pr " + repo)

    get_pr_timeline("facebook/react-native",135)
    # for repo in args.input:
    #     data = repo.strip().split(",")
    #     print(data)
    #     repo = data[0]
    #     pullrequest_id = data[1]
    #     try:
    #         get_pr_timeline(repo, pullrequest_id)
    #     except:
    #         raise
    #     print(repo)




