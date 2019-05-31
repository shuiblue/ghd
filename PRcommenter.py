import json
import requests

# Authentication info

with open('./input/parameter.txt') as f:
    USERNAME, TOKEN = f.read().splitlines()

# The repository to add this issue to
REPO_OWNER = 'akinnae'
REPO_NAME = 'curly-train'
PR_NUMBER = 1

#def make_github_comment(body=None, commit_id=None, path=None, position=None):
def make_github_comment(body=None):
    '''Create a comment on github.com using the given parameters.'''
    # Our url to create comments via POST
    url = 'https://api.github.com/repos/%s/%s/issues/%i/comments' % (REPO_OWNER, REPO_NAME, PR_NUMBER)
    # Create an authenticated session to create the comment
    headers = {
        "Authorization": "token %s" % TOKEN,
#        "Accept": "application/vnd.github.golden-comet-preview+json"
    }
#    session = requests.session(auth=(USERNAME, TOKEN))
    # Create our comment
    data = {'body': body}

    r = requests.post(url, json.dumps(data), headers=headers)
    if r.status_code == 201:
        print('Successfully created comment "%s"' % body)
    else:
        print('Could not create comment "%s"' % body)
        print('Response:', r.content)

make_github_comment('comment2')
