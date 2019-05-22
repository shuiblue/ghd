#!/usr/bin/python

import argparse
import pandas as pd

from common import decorators as d
import scraper

import MySQLdb

api = scraper.GitHubAPI()

fs_cache = d.fs_cache('shurui_commitFiles')



@fs_cache
def get_commit_changedFile(repo, issue):
    print("get reference of pr %s from %s" % (repo, issue))
    return pd.DataFrame(api.commit_changedFile(repo, issue))


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
        cnx = MySQLdb.connect(user='shuruiz', passwd='shuruiz', host='127.0.0.1',
                               port=3306)  # feature6
                               # port=3307)  # local
        cursor = cnx.cursor()

        # query = ("SELECT prc.sha FROM fork.PR_Commit_map prc RIGHT JOIN fork.Final f ON prc.projectID = f.repoID WHERE f.repoURL = %s AND prc.sha NOT in(SELECT fork.commit_files_GHAPI.sha  FROM fork.commit_files_GHAPI)")
        query = ("SELECT prc.sha FROM fork.PR_Commit_map prc   RIGHT JOIN fork.Final f ON prc.projectID = f.repoID   RIGHT JOIN fork.Pull_Request pr on prc.projectID = pr.projectID and prc.pull_request_id = pr.pull_request_ID WHERE f.repoURL =  %s and pr.closed = 'true' AND prc.sha NOT IN (SELECT fork.commit_files_GHAPI.sha FROM fork.commit_files_GHAPI) ")
        cursor.execute(query, [repo])

        for (sha) in cursor:
            print("%s : %s" % (repo, sha[0]))
            get_commit_changedFile(repo, sha[0])
        cursor.close()
        cnx.close()










