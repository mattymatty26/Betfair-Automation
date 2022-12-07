## Betfair Automation

This repository create an automated process to place bets on Betfair using the Betfair API.

You can create an API Account from instructions here https://betfair-datascientists.github.io/api/apiPythontutorial/.

Once you have created an appkey you can embed the key, your username and password in a .env file in the home base. An example of how a trade us executed and how we lofin is found in yhr trade_execution.py file. All helper functions are found in the Driver folder.  

To get around a "TypeError: Type is not JSON serializable: numpy.float64" caused within the betfairlightweightAPI, I extended the JSONEncoder in the betfairlightweight/endpoints/baseendpoint.py file as per the top answer here https://stackoverflow.com/questions/50916422/python-typeerror-object-of-type-int64-is-not-json-serializable
