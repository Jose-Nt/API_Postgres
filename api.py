#==========API desenvolvida com o objetivo de adquirir todos os dados de uma tabela específica em um banco de dados postgres==========#
from flask import Flask, jsonify, request
import pandas as pd
import json
from sqlalchemy import create_engine

#========== Consulta ao banco de dados ==========#
#=== OBS: No endereço da consulta, os parâmetros devem ser passados na ordem descrita abaixo.
#=== Usuários/Senha/Host/Base_de_dados/esquema/Tabela
app = Flask(__name__)
    
@app.route('/consulta/<usuario>/<senha>/<host>/<base_de_dados>/<schema>/<nome_tabela>', methods=['GET'])
def obter_dados(usuario, senha, host, base_de_dados, schema, nome_tabela):
    #===Estabelecendo conexão com banco de dados
    host = f'{host}'
    dbname = f'{base_de_dados}'
    user = f'{usuario}'
    password = f'{senha}'

    db_url = f"postgresql://{user}:{password}@{host}:5432/{dbname}"
    engine = create_engine(db_url)
    conexao = engine.connect()

    #===Realizando consulta e criando dataframe com os dados
    ponto = '.'
    consulta_sql = f'SELECT * FROM {schema}{ponto}{nome_tabela} LIMIT 10'
    df = pd.read_sql_query(consulta_sql, conexao)

    #===Redefinindo tipos de dados não aceitos por arquivos json    
    tipos = df.dtypes.tolist()
    colunas = df.columns.tolist()
    flag1 = False
    flag2 = False
    flag3 = False

    for i in range(len(tipos)):
        if tipos[i] == 'float64':
            df[colunas[i]] = df[colunas[i]].astype(float)
            flag1 = True
        if tipos[i] == 'int64':
            df[colunas[i]] = df[colunas[i]].astype(int)
            flag2 = True
        if tipos[i] == 'bool':
            df[colunas[i]] = df[colunas[i]].astype(bool)
            flag3 == True
        if (flag1 == False) and (flag2 == False) and (flag3 == False):
            df[colunas[i]] = df[colunas[i]].astype(str)
            
        flag1 = False
        flag2 = False
        flag3 = False

    #===Colocando os dados dentro de um dicionario e os retornando como json
    dicionario = df.to_dict()
    return jsonify(dicionario)
    
app.run(port=5000,host='localhost',debug=True)