# Databricks notebook source
# Import required libraries 
%pip install databricks-sql-connector
%pip install sdkms
#dbutils.library.restartPython()

import sdkms
from ctypes import string_at
from email import message
from pydoc import plain
import requests
import json
import sys
import base64
import os
from databricks import sql

# Define Variables:

# Provide Fortanix DSM details:
# api_key = dbutils.secrets.get(scope="<secret_scope_name>", key="<secret_key>")
# dsm_endpoint = "<DSM API Endpoint>"

# Provide SQL Warehouse connection details
# server_hostname = "<server>.cloud.databricks.com"
# http_path = "/sql/<path>"
# access_token = "<valid token>"

# Provide Fortanix DSM details:
api_key = dbutils.secrets.get(scope="hr_scope", key="FORTANIX_API_KEY")
dsm_endpoint = "apac.smartkey.io"

# Provide SQL Warehouse connection details
server_hostname_value = ""
http_path_value = ""
access_token_value = ""


# Cryto operation functions
def authenticate():
    url = "https://"+dsm_endpoint+"/sys/v1/session/auth"
    payload = ""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + api_key
    }
    response = requests.post(url, headers=headers, data=payload)
    return response.json().get('access_token')


def tokenize(bearer_token, message, key_id):
    url = "https://"+dsm_endpoint+"/crypto/v1/encrypt"
    
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
      
    payload = json.dumps({
        "key": {
            "kid": key_id 
        },
        "alg": "AES",
        "mode": "FPE",
        "plain": base64_message
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' +  bearer_token
    }

    response = requests.post(url, headers=headers, data=payload)
    print("\nTokenization operation - Response from DSM :" + response.text)
    return response.json().get('cipher')

def detokenize(bearer_token, tokenized_string, key_id):
    url = "https://"+dsm_endpoint+"/crypto/v1/decrypt"
    
    payload = json.dumps({
      "key": {
        "kid": key_id 
      },
      "alg": "AES",
      "mode": "FPE",
      "cipher": tokenized_string
    })
    
    headers = {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' +  bearer_token
    }

    response = requests.post(url, headers=headers, data=payload)
    print("\n\nDetokenization operation - Response from DSM :" + response.text)
    return response.json().get('plain')

# Authenticate to DSM using API Key
bearer_token = authenticate()

def tokenize_data(string_to_tokenize,key_id):# tokenize<bearer_token, string_to_tokenize>
    tokenized_string = tokenize(bearer_token, string_to_tokenize,key_id)
    base64_bytes = base64.b64decode(tokenized_string)
    return (base64_bytes.decode("utf-8"))

def detokenize_data(tokenized_string,key_id):# detokenize<bearer_token, cipher_to_detokenize>
    message_bytes = tokenized_string.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
    detokenized_string = detokenize(bearer_token, base64_message,key_id)
    base64_bytes = base64.b64decode(detokenized_string)
    return (base64_bytes.decode("utf-8"))



# Databricks SQL Warehouse functions:

def get_connection():
    conn = sql.connect(
                        server_hostname = server_hostname_value,
                        http_path = http_path_value,
                        access_token = access_token_value)
    return conn


def database():
  conn = get_connection()
  query ="SHOW DATABASES"
  cursor = conn.cursor()
  cursor.execute(query)
  db = cursor.fetchall()
  db_ls = [i['databaseName'] for i in db]
  cursor.close()
  return db_ls


def tables(db):
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("USE "+db)
  cursor.execute("SHOW TABLES")
  tables = cursor.fetchall()
  table_ls = [i['tableName'] for i in tables]
  cursor.close()
  return table_ls


def column_name(db,table):
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("USE "+db)
  cursor.execute("SHOW COLUMNS IN "+table)
  col = cursor.fetchall()
  col_ls = [i["col_name"] for i in col]
  cursor.close()
  return col_ls


def get_table_data(column, db, table):
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("USE "+db)
  # cursor.execute("SELECT * FROM tokenized_experiment")
  cursor.execute("SELECT * FROM "+table)
  data = cursor.fetchall()
  cursor.close()
  return data


def get_datatype(db, table, column):
  schema_dict = {}
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("USE "+db)
  # cursor.execute("SELECT "+column+" FROM "+table)
  cursor.execute("DESCRIBE "+table)
  schema = cursor.fetchall()
  cursor.close()
  for i in schema:
    schema_dict[i['col_name']] = i['data_type']
  return schema_dict



# Column tokenize and detokenize sample functions
def insert_tokenizedData(db, table, column, schema_dict, data_ls):
  try:
    query = "DROP TABLE "+"tokenized_"+table
    tok_table = "tokenized_"+table
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("USE "+db)
    cursor.execute(query)
    cursor.close()
    query = "CREATE TABLE "+tok_table+" ("+", ".join(f'{k} {v}' for k,v in schema_dict.items())+")"
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("USE "+db)
    cursor.execute(query)
    cursor.close()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("USE "+db)
    for i in data_ls:
      columns = list(i.keys())
      values = list(i.values())
      values = [str(i) for i in values]
      query = "INSERT INTO "+tok_table+" ("+", ".join(columns)+")"+" VALUES"+" "+"("+"'"+"', '".join(values)+"'"+")"
      # print(query)
      cursor.execute(query)
    cursor.close()
  except:
    tok_table = "tokenized_"+table
    query = "CREATE TABLE "+tok_table+" ("+", ".join(f'{k} {v}' for k,v in schema_dict.items())+")"
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("USE "+db)
    cursor.execute(query)
    cursor.close()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("USE "+db)
    for i in data_ls:
      columns = list(i.keys())
      values = list(i.values())
      values = [str(i) for i in values]
      query = "INSERT INTO "+tok_table+" ("+", ".join(columns)+")"+" VALUES"+" "+"("+"'"+"', '".join(values)+"'"+")"
      # print(query)
      cursor.execute(query)
    cursor.close()


def validate_col(a,b):
    for i in a:
        if i in b:
            return True

def tokenize_col(db,table,columns,keys):
  db_ls = database()
  if db in db_ls:
    table_ls = tables(db)
    if table in table_ls:
      col_ls = column_name(db,table)
      if validate_col(columns,col_ls):
        data = get_table_data(columns, db, table)
        schema_dict = get_datatype(db, table, columns)
        # print(data)
        # print(schema_dict)
        # tok_data = {}
        data_ls = []
        for k in range(len(data)):
          temp_dict = {}
          for i,j in zip(schema_dict.keys(), list(data[k])):
            temp_dict[i] = j
          data_ls.append(temp_dict)
        # final_data_ls = []
        for i,key in zip(columns,keys):
          for j in data_ls:
            j[i] = tokenize_data(j[i],key)
          # final_data_ls.append(i)
        
        insert_tokenizedData(db, table, columns , schema_dict, data_ls)

        # return data_ls,schema_dict
        return data_ls   
    

def detokenize_col(db, tok_table, columns,keys):
  conn = get_connection()
  cursor = conn.cursor()
  cursor.execute("USE "+db)
  cursor.execute("SELECT * from "+tok_table)
  tok_data = cursor.fetchall()
  data = get_table_data(columns, db, tok_table)
  schema_dict = get_datatype(db, tok_table, columns)
  # tok_data = {}
  data_ls = []
  for k in range(len(data)):
    temp_dict = {}
    for i,j in zip(schema_dict.keys(), list(data[k])):
      temp_dict[i] = j
    data_ls.append(temp_dict)
  # final_data_ls = []
  for i,key in zip(columns,keys):
    for j in data_ls:
      j[i] = detokenize_data(j[i],key)

  cursor.close()
  return data_ls

