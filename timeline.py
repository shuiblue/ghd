import requests


def timeline(repo, issue_id):
    # response = requests.get('https://api.github.com/repos/'+repo+'/issues/' + issue_id + '/timeline',
    #                         headers={'Accept': 'application/vnd.github.mockingbird-preview'})
    response = requests.get('https://api.github.com/repos/scikit-learn/scikit-learn/issues/11261/timeline',
                            headers={'Accept': 'application/vnd.github.mockingbird-preview'})

    print(response.status_code)
    payload = response.json()
    # print payload
    for event in payload:
        payload = response.json()
        # print payload
        for event in payload:
            print
            event.keys()
            # print event.get('issue_url', '')
            if event.has_key('event'):
                # print event['event']
                if event['event'] == 'cross-referenced':
                    yield {
                        'event': event['event'],
                        'author': event.get('actor'),
                        'commit_id': event.get('commit_id'),
                        'created_at': event.get('created_at'),
                        'id': event.get('source', '').get('id')
                    }
                    # # print event.keys()
                    # print
                    # event.get('issue_url', '')
                    # if event.has_key('event'):
                    #     print
                    #     event['event']
                    #     if event['event'] == 'cross-referenced':
                    #         print
                    #         event['source']
                    #         print
                    #         event.get('body', '')
                    #         print
                    #         event.get('url', '')
                    #         print
                    #         event.get('html_url', '')
