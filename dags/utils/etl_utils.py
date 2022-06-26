# import pandas as pd
import requests, os
# import datetime
# import numpy as np
# import psycopg2
from datetime import datetime, timedelta
from airflow.models import Variable

from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.mssql_operator import MsSqlOperator 
from airflow.hooks.mssql_hook import MsSqlHook
# from sqlalchemy import create_engine
# from sqlalchemy.sql.type_api import Variant



'''
The faux data lake is to represent a cloud based storage like s3 or GCS
'''
file_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
create_table_query = r"""
    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'platinum_customers')
        BEGIN
            DROP TABLE platinum_customers
        END
    CREATE TABLE platinum_customers (
        user_id INTEGER PRIMARY KEY not null,
        total_purchase_value FLOAT not null,
        timestamp DATETIME NULL DEFAULT GETDATE()
    )
    
    IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'platinum_customers_per_product')
        BEGIN
            DROP TABLE platinum_customers_per_product
        END
    CREATE TABLE platinum_customers_per_product (
        user_id INTEGER PRIMARY KEY not null,
        product_name VARCHAR not null,
        total_purchase_value FLOAT not null,
        timestamp DATETIME NULL DEFAULT GETDATE()
    )
    """
    

def insert_mssql_hook(df, db_name=str, target_fields=list):
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_conn_id', schema='ecommerce')
    current_time = str(datetime.now())[0:-3]
    rows = list([(x[0], x[1], str(current_time)) for x in df.values.tolist()])
    # target_fields = ['user_id', 'total_purchase_value', 'timestamp']
    # target_fields = columns
    # mssql_hook.insert_rows(table='platinum_customers', rows=rows, target_fields=target_fields)
    mssql_hook.insert_rows(table=db_name, rows=rows, target_fields=target_fields)
    
# modeled as transformation jobs

def get_platinum_customer(): 
    '''
    a platinum customer has purchased goods worth over 5000 
    '''
    import pandas as pd
    transaction_data = pd.read_csv('/tmp/mongo_transaction_lean_customer_data.csv')
    user_data = pd.read_csv('/tmp/mongo_user_lean_customer_data.csv')
    user_tx_data = pd.merge(user_data,transaction_data, left_on='user_id', right_on='user_id')
    # also need product lean_customer_data for product price
    product_data = pd.read_csv('/tmp/mongo_product_lean_customer_data.csv')
    product_data = product_data[['product_id','price','product_name']]
    
    enriched_customer_data = pd.merge(user_tx_data,product_data)
    # get total value
    enriched_customer_data['total_purchase_value'] = enriched_customer_data['quantity'] * enriched_customer_data['price']
    #retain only the columns necessary for analysis
    lean_customer_data = enriched_customer_data[['user_id',
                                                     'total_purchase_value',
                                                     ]]
    # get total purchase value per customer
    lean_customer_data = lean_customer_data.groupby(['user_id']).sum().reset_index()
    # filter platinum customers PREDICATE: total_purchase_value>=10000
    platinum_customers = lean_customer_data.loc[lean_customer_data['total_purchase_value'] >= 10000]
    # save to csv file
    platinum_customers.to_csv('/tmp/platinum_customers.csv', index=False)
    # to database
    # _load_platinum_customers_to_db(platinum_customers)
    target_columns = ['user_id', 'total_purchase_value', 'timestamp']
    insert_mssql_hook(platinum_customers, 'platinum_customers', target_columns)
    
    
    # special case: FIND BIG SPENDER CUSTOMERS WITH TOTAL VALUE OF 5000 PER PRODUCT
    
    special_customer_data = enriched_customer_data[['user_id',
                                                     'total_purchase_value',
                                                     'product_name',
                                                     ]]
    
    special_customer_data = special_customer_data.groupby(['user_id','product_name']).sum().reset_index()
    
    special_customers = special_customer_data.loc[special_customer_data['total_purchase_value'] >= 5000]
    # save to csv file
    special_customers.to_csv('/tmp/platinum_customers_per_product.csv', index=False)
    target_columns = ['user_id','product_name','total_purchase_value', 'timestamp']
    insert_mssql_hook(special_customers, 'platinum_customers_per_product', target_columns)
    
    
    
def get_basket_analysis_dataset(): 
    '''
    group by purchase ID and store data
    '''
    import pandas as pd
    transaction_data = pd.read_csv('/tmp/mongo_transaction_lean_customer_data.csv')
    transaction_data = transaction_data[['product_id','quantity','purchase_id']]
    # group to have unique purchase ID
    grouped_data = transaction_data.groupby(['purchase_id','product_id']).count().reset_index()
    # pivot to have purchase ID as index, product IDs as columns and quantity(mean) as the values
    # for basket analysis, one is considering what goes together(cause and effect). So average is a better agg function
    grouped_data = pd.pivot_table(grouped_data,index='purchase_id', 
                   columns='product_id',
                   values='quantity', 
                   fill_value=0, # empty values that may arise from pivoting
                   ).add_suffix('_product_id')
    
    grouped_data.to_csv('/tmp/basket_analysis.csv', index=False)


def get_recommendation_engine_dataset(): 
    '''
    group by user ID and send to data lake as dataset
    '''
    import pandas as pd
    import numpy as np
    transaction_data = pd.read_csv('/tmp/mongo_transaction_lean_customer_data.csv')
    transaction_data = transaction_data[['user_id','quantity','product_id']]
    # pivot to have user ID as index, product IDs as columns and quantity(sum) as the values
    # for recommendation engines, it may be critical to know what kind of products are bought in large quantities over time
    transaction_data = pd.pivot_table(transaction_data,index='user_id', 
                   columns='product_id',
                   values='quantity', 
                   fill_value=0,
                   aggfunc=np.sum).add_suffix('_product_id')
    
    transaction_data.to_csv('/tmp/recommendation_engine_analysis.csv', index=False)
    

def pull_mongo_data(collection=str, min_ago=int): 
    hook = MongoHook(conn_id='mongodb_local_con_id')
    now = datetime.now()
    query = {
        "$expr":{
            "$and":[
                {"$gte":[{"$toDate":"$_id"}, datetime.now() - timedelta(minutes=min_ago)]},
                {"$lte":[{"$toDate":"$_id"}, now]}
            ]
        }
    }
    # query = {}
    print(query)
    return hook.find(mongo_collection=collection, query=query, find_one=False, mongo_db="ecommerce")


def pull_mongo_user_data():
    import pandas as pd 
    user_data = pull_mongo_data("user", 120)
    user_data = pd.DataFrame(list(user_data))
    user_data.loc[:, "address"] = user_data["address"].apply(lambda x : x.replace('\n', ' '))
    user_data.to_csv('/tmp/mongo_user_lean_customer_data.csv', index=False)

def pull_mongo_product_data(): 
    import pandas as pd
    product_data = pull_mongo_data("product", 120)
    product_data = pd.DataFrame(list(product_data))
    product_data.to_csv('/tmp/mongo_product_lean_customer_data.csv', index=False)

def pull_mongo_transaction_data(): 
    import pandas as pd
    transaction_data = pull_mongo_data("transaction", 120)
    transaction_data = pd.DataFrame(list(transaction_data))
    transaction_data.to_csv('/tmp/mongo_transaction_lean_customer_data.csv', index=False)


def pull_user_data(): 
    import pandas as pd
    user_data = requests.get(f'{Variable.get("API_URL")}/users')
    user_data = pd.DataFrame(user_data.json())
    user_data.loc[:, "address"] = user_data["address"].apply(lambda x : x.replace('\n', ' '))
    user_data.to_csv('/tmp/user_lean_customer_data.csv', index=False)

def pull_product_data(): 
    import pandas as pd
    product_data = requests.get(f'{Variable.get("API_URL")}/products')
    product_data = pd.DataFrame(product_data.json())
    product_data.to_csv('/tmp/product_lean_customer_data.csv', index=False)

def pull_transaction_data():
    import pandas as pd 
    transaction_data = requests.get(f'{Variable.get("API_URL")}/transactions')
    transaction_data = pd.DataFrame(transaction_data.json())
    transaction_data.to_csv('/tmp/transaction_lean_customer_data.csv', index=False)

print(file_root)
