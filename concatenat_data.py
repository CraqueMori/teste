import pandas as pd
import time
import sqlalchemy

import importlib
import global_keys
import global_aux
import numpy as np
import re
import os

# Carregando funções redefinidas
importlib.reload(global_aux)

# Configuração global
CREDENTIALS = global_keys.get_database_credentials()
SCHEMA = 'teste_scripts_06_04'
IF_EXISTS = 'replace'

start_time = time.time()
engine = sqlalchemy.create_engine(
    f"postgresql://{CREDENTIALS['user']}:{CREDENTIALS['password']}@{CREDENTIALS['host']}:{CREDENTIALS['port']}/{CREDENTIALS['database']}",
    pool_pre_ping=True
)
directory = 'Export 23.05.24'
# Lista para armazenar cada DataFrame lido
lista_dfs = []

# Loop para ler cada arquivo CSV na pasta
for arquivo in os.listdir(directory):
    caminho_completo = os.path.join(directory, arquivo)
    if arquivo.endswith('.csv'):
        df = pd.read_csv(caminho_completo)
        lista_dfs.append(df)

# Concatena todos os DataFrames na lista
df_concatenado = pd.concat(lista_dfs, ignore_index=True)

#df_emails = pd.read_excel('emails.xlsx' ,engine='openpyxl')



#df_final = pd.merge(df_concatenado, df_emails, on='Escola', how='left')
# Salva o DataFrame concatenado em um novo arquivo CSV
df_concatenado.to_excel('concatenacao_23_05.xlsx',engine='openpyxl', index=False)


# global_aux.write2sql(
#     dataframe=df_concatenado, 
#     table_name='d_dashboard_teste', 
#     database_connection=engine,
#     database_schema=SCHEMA,
#     if_exists=IF_EXISTS
# )



end_time = time.time()
print('CONCLUIDO')
print(f'Tempo total do script: {round((end_time - start_time) / 60, 4)} min')

# df_simulado_104 = df_concatenado[df_concatenado['Ano'] == "4 ANO"]
# df_simulado_108 = df_concatenado[df_concatenado['Ano'] == "8 ANO"]

# df_simulado_104.to_excel('dados_4_ano.xlsx', engine="openpyxl")
# df_simulado_108.to_excel('dados_8_ano.xlsx', engine="openpyxl")