import os

os.environ['environment'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

environment = os.environ['environment']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

applicationName = 'Steam_games_analysis'

current_dir = os.getcwd()

src_olap = current_dir + '\source\olap'

src_oltp = current_dir + '\source\oltp'
