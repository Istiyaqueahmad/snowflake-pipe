import pandas as pd
import snowflake.connector as sf
import json, os
import time

from flask import Flask, request
app = Flask(__name__)

global conn

snowflake_username = os.environ['SNOWFLAKE_USERNAME']
snowflake_password = os.environ['SNOWFLAKE_PASSWORD']
snowflake_account = os.environ['SNOWFLAKE_ACCOUNT']

snowflake_warehouse = 'COMPUTE_WH'
database_name = 'DEMO_DB'
schema_name = 'EMPLOYEE'
snowflake_role = 'ACCOUNTADMIN'

conn = sf.connect(user = snowflake_username,
                password = snowflake_password,
                account = snowflake_account)

def run_query(connection,query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

try:
    warehouse_sql = 'use warehouse {}'.format(snowflake_warehouse)
    run_query(conn, warehouse_sql)
    
    try:
        sql = 'alter warehouse {} resume'.format(snowflake_warehouse)
        run_query(conn, sql)
    except:
        pass
    
    sql = 'use database {}'.format(database_name)
    run_query(conn, sql)
    
    sql = 'use role {}'.format(snowflake_role)
    run_query(conn, sql)
    
    sql = f'use schema {schema_name}'
    run_query(conn, sql)

except Exception as e:
    print(e)
    
def customers_data(market_segment, conn):
    global customers
    sql = f"""select * 
            from CUSTOMERS
            where upper(C_MKTSEGMENT) = upper('{market_segment}') limit 200 ;"""
    customers = pd.read_sql(sql, conn)
    # data processing
    # Fetch only those customers who have Account Balance greater than 5000
    customers_processed = customers[customers['C_ACCTBAL'] > 5000]
    return {"data":json.loads(customers_processed.to_json(orient='records'))}

@app.route("/customers-data", methods = ['GET', 'POST'])
def expose_customers_data():
    if request.method == 'POST':
        input_json = request.get_json()
        if 'market_segment' in input_json.keys():
            market_segment = str(input_json['market_segment'])
        else:
            return "Error: No Market Segment field is provided. Please specify a Market Segment to filter the data."
        
    customers = customers_data(market_segment, conn)
    return customers


if __name__ == '__main__':
    app.run(debug=True, use_reloader = False)
    
    
    
{
  "market_segment": "BUILDING"
}