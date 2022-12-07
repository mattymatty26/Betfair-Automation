from Driver.functions import *
import betfairlightweight
from betfairlightweight import filters
import pandas as pd
import numpy as np
import os
import datetime
import json
import logging
import bz2
import tarfile
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO)

#load Betfair API ID and betfair account details
load_dotenv()

appkey = os.getenv("appkey")
user = os.getenv("user")
password = os.getenv("password")
trading =  betfairlightweight.APIClient(user,password,app_key=appkey)
trading.login_interactive()



bet_size = 5.00
market_id='1.207303789' # Match Odds on Brazil vs South Korea
selection_id = 1408 #Brazil 

make_order_best_price(bet_size,market_id,selection_id,trading)
