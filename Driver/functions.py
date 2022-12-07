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



def get_competitions(sport_id,days=7):
    """This function returns all competitions for a chosen sport within a time frame

    Args:
        sport_id (int): id is the betfair event ID which is reflective of the sport
        days (int) (optional) : The time frame in which the games will be searched for 
        
        
    Returns:
        DataFrame: of all matches within the sport
            
    """
    # Get a datetime object in a week and convert to string
    datetime_in_a_week = (datetime.datetime.utcnow() + datetime.timedelta(days=days)).strftime("%Y-%m-%dT%TZ")

    # Create a competition filter
    competition_filter = betfairlightweight.filters.market_filter(
        event_type_ids=[sport_id], # Soccer's event type id is 1
        market_start_time={
            'to': datetime_in_a_week
        })

    # Get a list of competitions for chosen sport 
    competitions = trading.betting.list_competitions(
        filter=competition_filter
    )

    # Iterate over the competitions and create a dataframe of competitions and competition ids
    df = pd.DataFrame({
        'Competition': [competition_object.competition.name for competition_object in competitions],
        'ID': [competition_object.competition.id for competition_object in competitions]
    })
    
    return df
    

    
    
def allevents(sport_id,days = 7):
    """This function returns all events (games) for a chosen sport within a time frame

    Args:
        id (int): id is the betfair event ID which is reflective of the sport 
        days (int) (optional) : The time frame in which the games will be searched for     
        
    Returns:
        DataFrame: of all events within the sport    
        
    """
    
    # Get a datetime object in a week and convert to string
    datetime_in_a_week = (datetime.datetime.utcnow() + datetime.timedelta(days=days)).strftime("%Y-%m-%dT%TZ")
    
    # Define a market filter
    Event_filter = betfairlightweight.filters.market_filter(
        event_type_ids=['{}'.format(sport_id)],      
        market_start_time={
            'to': datetime_in_a_week    
    })

    # Get a list of all thoroughbred events as objects
    events = trading.betting.list_events(
        filter=Event_filter
    )

    # Create a DataFrame with all the events by iterating over each event object
    df = pd.DataFrame({
        'Event Name': [event_object.event.name for event_object in events],
        'Event ID': [event_object.event.id for event_object in events],
        'Event Venue': [event_object.event.venue for event_object in events],
        'Country Code': [event_object.event.country_code for event_object in events],
        'Time Zone': [event_object.event.time_zone for event_object in events],
        'Open Date': [event_object.event.open_date for event_object in events],
        'Market Count': [event_object.market_count for event_object in events]
    })

    return df

def event_catalogue(event_ids,limit = 100):
    
    """ This function returns a catalog of all the markets within an event.
    Args:
        event_ids (_list_): A list of all event ID's representing an individual match in Betfair
        limit (_int_) (optional):The amount of markets returned based on the most traded markets.  
        
    Returns:
        _DataFrame_: The top markets, their corresponding market ID and the total dollar value traded within that market
    """
    
    market_catalogue_filter = betfairlightweight.filters.market_filter(event_ids=event_ids)

    market_catalogues = trading.betting.list_market_catalogue(
        filter=market_catalogue_filter,
        max_results=str(limit),
        sort='MAXIMUM_TRADED'
    )

    # Create a DataFrame for each market catalogue
    df = pd.DataFrame({
        'Market Name': [market_cat_object.market_name for market_cat_object in market_catalogues],
        'Market ID': [market_cat_object.market_id for market_cat_object in market_catalogues],
        'Total Matched': [market_cat_object.total_matched for market_cat_object in market_catalogues],
    })

    return df

def process_runner_books(runner_books):
    """This function processes the runner books and returns a DataFrame with the best back/lay prices + vol for each runner
    Args:
        runner_books (_BetFairMarketBook_): A betfair market book refer to betfair API https://docs.developer.betfair.com/display/1smk3cen4v3lu3yomq5qye0ni/listMarketBook

    Returns:
        DataFrame: A data frame with the best back and lay prices for each individual market
    """

    best_back_prices = [runner_book.ex.available_to_back[0].price
                        if runner_book.ex.available_to_back
                        else 1.01
                        for runner_book
                        in runner_books]
    best_back_sizes = [runner_book.ex.available_to_back[0].size
                       if runner_book.ex.available_to_back
                       else 1.01
                       for runner_book
                       in runner_books]

    best_lay_prices = [runner_book.ex.available_to_lay[0].price
                       if runner_book.ex.available_to_lay
                       else 1000.0
                       for runner_book
                       in runner_books]
    best_lay_sizes = [runner_book.ex.available_to_lay[0].size
                      if runner_book.ex.available_to_lay
                      else 1.01
                      for runner_book
                      in runner_books]
    
    selection_ids = [runner_book.selection_id for runner_book in runner_books]
    last_prices_traded = [runner_book.last_price_traded for runner_book in runner_books]
    total_matched = [runner_book.total_matched for runner_book in runner_books]
    statuses = [runner_book.status for runner_book in runner_books]
    scratching_datetimes = [runner_book.removal_date for runner_book in runner_books]
    adjustment_factors = [runner_book.adjustment_factor for runner_book in runner_books]

    df = pd.DataFrame({
        'Selection ID': selection_ids,
        'Best Back Price': best_back_prices,
        'Best Back Size': best_back_sizes,
        'Best Lay Price': best_lay_prices,
        'Best Lay Size': best_lay_sizes,
        'Last Price Traded': last_prices_traded,
        'Total Matched': total_matched,
        'Status': statuses,
        'Removal Date': scratching_datetimes,
        'Adjustment Factor': adjustment_factors
    })
    return df



def make_order_best_price(betsize,market_id,selection_id,trading):
    """Creates a Betfair Limit order at the best price .Based on a bet size on a specific market on a specific selection.

    Args:
        betsize (_float_): Size of the bet stake eg. 5.00
        market_id (_str_): Market Id from betfair
        selection_id (_int_): Selection ID from betfair
        trading (_betfairlightweight.apiclient.APIClient_): A Betfair logged in API client
        
    Returns None    
    """
    # Create a price filter. Get all traded and offer data
    price_filter = betfairlightweight.filters.price_projection(
        price_data=['EX_BEST_OFFERS']
    )


    # Request market books
    market_books = trading.betting.list_market_book(
        market_ids = [market_id],
        price_projection=price_filter
    )

    # Grab the first market book from the returned list as we only requested one market 
    market_book = market_books[0]

    runners_df = process_runner_books(market_book.runners)
    
    price_odds = runners_df.loc[runners_df["Selection ID"]==selection_id]['Best Back Price'].values[0]
    
    # Define a limit order filter
    limit_order_filter = betfairlightweight.filters.limit_order(
            size=betsize, 
            price=price_odds,
            persistence_type='LAPSE'
        )

    # Define an instructions filter
    instructions_filter = betfairlightweight.filters.place_instruction(
        order_type="LIMIT",
        selection_id=selection_id,
        side="BACK",
        limit_order=limit_order_filter
    )
    
    logging.info("Placing order")
    # Place the order
    order = trading.betting.place_orders(
        market_id=market_id, # The market id we obtained from before
        customer_strategy_ref='Test', #comment
        instructions=[instructions_filter] # This must be a list
    )
    
    logging.info(order.__dict__)

    