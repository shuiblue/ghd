import subprocess
import time
import scratchpad
import pandas as pd

# subprocess.call(['python', 'getCommits.py','-i','repoList.txt'])
subprocess.call(['python', 'scratchpad.py','-i','repoList.txt'])

# subprocess.call(['python', 'missPR.py','-i','repoList.txt'])
