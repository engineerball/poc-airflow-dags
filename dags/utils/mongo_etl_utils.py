# import pandas as pd
import requests, os
# import numpy as np
# import psycopg2
from datetime import datetime, timedelta
from airflow.models import Variable

from airflow.providers.mongo.hooks.mongo import MongoHook

# from sqlalchemy import create_engine
# from sqlalchemy.sql.type_api import Variant



'''
The faux data lake is to represent a cloud based storage like s3 or GCS
'''
file_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
create_table_query = """
    CREATE TABLE IF NOT EXISTS platinum_customers(
        user_id INTEGER PRIMARY KEY not null,
        total_purchase_value FLOAT not null,
        timestamp date not null default CURRENT_DATE
    )
    """
    
    
# modeled as loading job
def _load_platinum_customers_to_db(df): 
    # import psycopg2
    # connection = psycopg2.connect(user=Variable.get("POSTGRES_USER"),
    #                               password=Variable.get("POSTGRES_PASSWORD"),
    #                               host="remote_db",
    #                               database=Variable.get("DB_NAME"))
     
    # cursor = connection.cursor()
    # # Print PostgreSQL details
    # print("PostgreSQL server information")
    # print(connection.get_dsn_parameters(), "\n")


    import pymssql
    connection = pymssql.connect(server=f'{Variable.get("MSSQL_HOST")}', 
                                 user=f'{Variable.get("MSSQL_USER")}', 
                                 password=f'{Variable.get("MSSQL_PASSWORD")}', 
                                 database=f'{Variable.get("MSSQL_DB")}')       
    
    cursor = connection.cursor()  

    try:
        df.to_sql("platinum_customers", connection, index=False,if_exists='append')

        
    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    

# modeled as transformation jobs

def get_platinum_customer(): 
    '''
    a platinum customer has purchased goods worth over 5000 
    '''
    import pandas as pd
    transaction_data = pd.read_csv(os.path.join(file_root,'../data/mongo_transaction_lean_customer_data.csv'))
    user_data = pd.read_csv(os.path.join(file_root,'../data/mongo_user_lean_customer_data.csv'))
    user_tx_data = pd.merge(user_data,transaction_data, left_on='user_id', right_on='user_id')
    # also need product lean_customer_data for product price
    product_data = pd.read_csv(os.path.join(file_root,'../data/mongo_product_lean_customer_data.csv'))
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
    platinum_customers.to_csv(os.path.join(file_root,'../data/platinum_customers.csv'), index=False)
    # to database
    _load_platinum_customers_to_db(platinum_customers)
    
    
    # special case: FIND BIG SPENDER CUSTOMERS WITH TOTAL VALUE OF 5000 PER PRODUCT
    
    special_customer_data = enriched_customer_data[['user_id',
                                                     'total_purchase_value',
                                                     'product_name',
                                                     ]]
    
    special_customer_data = special_customer_data.groupby(['user_id','product_name']).sum().reset_index()
    
    special_customers = special_customer_data.loc[special_customer_data['total_purchase_value'] >= 5000]
    # save to csv file
    special_customers.to_csv(os.path.join(file_root,'../data/platinum_customers_per_product.csv'), index=False)
    
    
    
def get_basket_analysis_dataset(): 
    '''
    group by purchase ID and store data
    '''
    import pandas as pd
    transaction_data = pd.read_csv(os.path.join(file_root,'../data/mongo_transaction_lean_customer_data.csv'))
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
    
    grouped_data.to_csv(os.path.join(file_root,'../data/basket_analysis.csv'), index=False)


def get_recommendation_engine_dataset(): 
    '''
    group by user ID and send to data lake as dataset
    '''
    import pandas as pd
    import numpy as np
    transaction_data = pd.read_csv(os.path.join(file_root,'../data/mongo_transaction_lean_customer_data.csv'))
    transaction_data = transaction_data[['user_id','quantity','product_id']]
    # pivot to have user ID as index, product IDs as columns and quantity(sum) as the values
    # for recommendation engines, it may be critical to know what kind of products are bought in large quantities over time
    transaction_data = pd.pivot_table(transaction_data,index='user_id', 
                   columns='product_id',
                   values='quantity', 
                   fill_value=0,
                   aggfunc=np.sum).add_suffix('_product_id')
    
    transaction_data.to_csv(os.path.join(file_root,'../data/recommendation_engine_analysis.csv'), index=False)
    

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
    user_data.to_csv(os.path.join(file_root,'../data/mongo_user_lean_customer_data.csv'), index=False)

def pull_mongo_product_data(): 
    import pandas as pd
    product_data = pull_mongo_data("product", 120)
    product_data = pd.DataFrame(list(product_data))
    product_data.to_csv(os.path.join(file_root,'../data/mongo_product_lean_customer_data.csv'), index=False)

def pull_mongo_transaction_data(): 
    import pandas as pd
    transaction_data = pull_mongo_data("transaction", 120)
    transaction_data = pd.DataFrame(list(transaction_data))
    transaction_data.to_csv(os.path.join(file_root,'../data/mongo_transaction_lean_customer_data.csv'), index=False)


print(file_root)
