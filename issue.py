#!/usr/bin/python

import argparse
import pandas as pd

from common import decorators as d
import scraper

api = scraper.GitHubAPI()

fs_cache = d.fs_cache('shurui_crossRef')


@fs_cache
def get_issue_timeline(repo, issue):
    print("get reference of issue/pr %s from %s" % (repo, issue))
    return pd.DataFrame(api.issue_pr_timeline(repo, issue))

@fs_cache
def get_issues(repo):
    print("get issue from " + repo)
    return pd.DataFrame(api.repo_issues(repo))


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

    for repo in args.input:
        repo = repo.strip()
        try:
            issues = get_issues(repo)
        except:
            raise
        print(repo)
        if len(issues):
            for issue_id in issues['number']:
                if RepresentsInt(issue_id):
                    issue_id = int(issue_id)
                    # commits = get_pr_commits(repo, pullrequest_id)
                    # comments = get_pr_comments(repo, pullrequest_id)
                    get_issue_timeline(repo, issue_id)

        else:
            print(" no issue " + repo)
