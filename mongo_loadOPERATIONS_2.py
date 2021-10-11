# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 20:15:04 2021

@author: munozher
"""

import pymongo
from pymongo import MongoClient, errors
import pandas as pd
from pandas import DataFrame
import numpy as np
import json
from pymongo.mongo_client import MongoClient
#from pymongo.server_api import ServerApi
#DEFINO PARAMETROS DE CONEXION
MONGODB_HOST = '127.0.0.1'
MONGODB_PORT = 27017
DB_NAME = 'RSData'
#recordings = 'RECORDINGS'
# Provide the mongodb atlas url to connect python to mongodb using pymongo
#CONNECTION_STRING = "mongodb://127.0.0.1:27017/RSData"
#from pymongo import MongoClient
#client = MongoClient(CONNECTION_STRING)
#client = MongoClient("127.0.0.1",27017)   
client = MongoClient(MONGODB_HOST,MONGODB_PORT)   
print ("server version:", client.server_info()["version"])
#db = client.DB_NAME
#try:
#    db = client.RSData
#except errors.ServerSelectionTimeoutError as err:
#    print("Unable to connect to the server.")
#    print ("pymongo ERROR:", err)
database_names = client.list_database_names()
#print ("database_names" "TYPE:", type(database_names))
#print ("The client's list_database_names() method returned", len(database_names), "database names.")
#############
# iterate over the list of database names
for db_num, db in enumerate(database_names):

    # print the database name
    print ("Base de Datos: ", db, " /// Número de Colecciones: ", db_num)

    # use the list_collection_names() method to return collection names
    collection_names = client[db].list_collection_names()
    #print ("list_collection_names() TYPE:", type(database_names))
    #print ("The MongoDB database returned", len(collection_names), "collections.")
############
basedatos=input("Nombre de la base de datos: ")
collection_names = client[basedatos].list_collection_names()
    
opcion = input("elige una de las opciones (1) leer tabla (2) insertar tabla (3) Borrar tabla: ")
print(f"tu opcion es: {opcion}")
if opcion=="1":
    try:
        db = client.RSData
    except:
        print("Unable to connect to the server.")
    for col_num, col in enumerate(collection_names):
        print ("Coleccion: ", col)
    coleccion=input("elige una tabla:")
    #coleccion=coleccion.upper()
    count=db[coleccion].count_documents({})
    print(f"has elegido la coleccion: {coleccion} /// tiene {count} registros")
    print(coleccion)
    #recordings = db.RECORDINGS
    #recordings = db.coleccion
    #print(recordings)
    #test=recordings.find({'ChannelID': '4'})
    test=db[coleccion].find({})
    for doc in test:
        print(doc)
    test2=db[coleccion].find_one({})
    print(f"la estructura de {coleccion} es la siguiente:")
    print(test2)
    campo = input("elige un campo: ")
    valor = input( "elige un valor: ")
    #Esto lo hago para añadir la comilla simple y que mongo se lo trague:RSData
    campo=str(campo)
    valor=str(valor)
    documento1={campo:valor}
    print(f"documento1: {documento1}")
    print(f"coleccion: {coleccion}, campo {campo}, valor {valor}")
    #La siguiente esta mal porque no devuelve ningun resultado:
    test=db[coleccion].find(documento1)
    cuenta=db[coleccion].count_documents(documento1)
    #print(f"El número de registros con {campo} con valor {valor} es {cuenta}")
    for doc in test:
        print(doc)
    print(f"El número de registros con {campo} con valor {valor} es {cuenta}")
    #print(cuenta)
    #print(test)
    #data = pd.DataFrame(list(recordings.find()))
    #print(data)
    #df=pd.DataFrame(test)
    df = pd.DataFrame(list(db[coleccion].find({})),columns=['ChannelID','StartDate'])
    print(df.head())

elif opcion=="2":
    with open('OPERATIONS.json') as f:
        file_data = json.load(f)
        print(file_data)
    operations=pd.DataFrame(list(file_data))
    print(operations)
    cuenta="count"
    fecha="_id"
    column_sum=operations[cuenta].sum()
    column_sum=operations[cuenta].count()
    print(column_sum)
    dboperations=db['OPERATIONS']
    dboperations.insert_many(file_data)
elif opcion=="3":
    try:
        db = client.RSData
    except:
        print("Unable to connect to the server.")
    coleccion=input("elige una tabla:")
    db[coleccion].drop()
    print(f"Borrada colección:  {coleccion}")
    
else:
    print("Opción no soportada")
