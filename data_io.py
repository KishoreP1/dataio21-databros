# -*- coding: utf-8 -*-
"""
Created on Sat Oct 23 11:54:44 2021

@author: sreic
"""

import v2x_sdk as sdk
import pandas as pd

sdk.get_auth_code(sdk.PUBLIC,sdk.LOGIN_99P)
ctx = sdk.get_context(sdk.PUBLIC,sdk.LOGIN_99P)
#Enter Auth Code :
#dbfce5b8-f05c-41d8-b72b-0b689ca1b2...
ctx.set_query_type(sdk.SUMMARY)
#ctx.set_days(1000)
#ctx.set_limit(1000)
#df,err = sdk.get_dataframe(ctx)
def get_n_rows(sdk, context, n_rows):
    df_pages = []
    rows_to_collect = n_rows
    
    # get the first data frame
    sep = '=' * 100
    print(sep)
    row_limit = min([rows_to_collect, 50000])
    context.set_limit(row_limit)
    df,err = sdk.get_dataframe(context)
    df_pages.append(df)
    rows_to_collect -= row_limit
    print(context.get_query())
    print(f'\nFirst Query Ran for {row_limit} rows. There are {rows_to_collect} rows left to gather')
    key = df.loc[df.shape[0]-1,'key']
    print(f'\tFINAL PAGE KEY = {key.strip("=")}')
    print(sep)
    
    
    while rows_to_collect > 0:
        # setup custom query for next page
        row_limit = min([rows_to_collect, 50000])
        context = _get_next_page_query(context, row_limit, key)
        
        # run query
        df_temp,err = sdk.get_dataframe(context)
        if err is not None:
          print(err)
        df_pages.append(df_temp)
        rows_to_collect -= row_limit 
        # get the last key
        try:
            key = df_temp.loc[df_temp.shape[0]-1,'key']
        except KeyError:
            print("last data")
        print(f'\nNext Query Ran for {row_limit} rows. There are {rows_to_collect} rows left to gather')
        print(f'\tFINAL PAGE KEY = {key.strip("=")}')
        print(sep)
    
    return pd.concat(df_pages)
        
def _get_next_page_query(context, row_limit, key):
    query = context.get_query()
    
    payload = query['query']
    
    # Create the query prefix
    split1 = payload.split('next:{')
    prefix = split1[0] + 'next:{'
    # Create the query suffix
    split2 = split1[1].split('}')
    suffix = '} }' + split2[-3] + '}}'
    # Create the parameterize payload fetch statement
    payload_fetch = f' fetch:{row_limit} key:\"{key}\" '
    
    payload = prefix + payload_fetch + suffix
              
    query['query'] = payload
    context.set_custom_query(query)
    print(query)
    return context

get_n_rows(sdk, ctx, 500000)