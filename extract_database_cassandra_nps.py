from datetime import datetime as dtime
import datetime as dt
from cassandra.cluster import Cluster

#############################################################################################################################################
#### Conexão Cassandra
#############################################################################################################################################
def execute_query(query_text):
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    session.execute('use thingsboard; ')
    query = session.execute(query_text)

    cluster.shutdown()

    return query 

#############################################################################################################################################
#### Arquivo cassandra_queries.py
#############################################################################################################################################

###Metodo para contagem de numProd 
def select_database_nps(id_equipamento):
    d_h_now = dtime.now()
    d_now = dt.date(d_h_now.year, d_h_now.month, d_h_now.day)
    h_now = dt.time(d_h_now.hour, d_h_now.minute, d_h_now.second)
    h_finish = dt.time(17, 45, 00)

    d_yesterday = d_now - dt.timedelta(days=1)
    dt_yesterday = dtime.combine(d_yesterday, h_finish)

    d_current = dtime.combine(d_now, h_finish)
    
    filter_dat = ''
    
    if h_now <= h_finish:
        filter_dat = int((dtime.timestamp(dt_yesterday)) * 1000)
    
    elif h_now > h_finish:
        filter_dat = int((dtime.timestamp(d_current)) * 1000)

    query_text = 'SELECT nome_sinal, valor, id_equipamento, created_at ' \
                  'FROM streamsets_line_h ' \
                  'WHERE id_equipamento = ' + str(id_equipamento) + ' ' \
                  'AND created_at >= ' + str(filter_dat) + ' ALLOW FILTERING ;'
    query = execute_query(query_text)

    result = []
    if query.one() is not None:
        for line in query.current_rows:
            result.append({
                'nome_sinal': line.nome_sinal,
                'valor': line.valor,
                'id_equipamento': line.id_equipamento,
                'created_at': line.created_at
                })
        
        result.sort(key=lambda x: x['created_at'])
        
    return result

#Metodo para verificar se há algum equipamento na esteira de produção
def select_presence_product(id_equipamento):
    nome_sinal = "PresencaProduto"
    query_text =  \
        'SELECT max(created_at) as max_timestamp ' \
        'FROM streamsets_line_h ' \
        'WHERE  nome_sinal = \'' + str(nome_sinal) + '\' ' \
        'AND id_equipamento = '+ str(id_equipamento) + ' ALLOW FILTERING ;'
        
    query = execute_query(query_text)
    result = query.one().max_timestamp

    return result

#############################################################################################################################################
#### Arquivo productions.py
#############################################################################################################################################
#Verifica se existe algum equipamento sendo produzido nesse momento
def flag_presence_nps(table, id_equipamento):
    max_timestamp = select_presence_product(id_equipamento)
    
    fl_presence = False
    for line in table:
        if line["created_at"] == max_timestamp:
            fl_presence = True

    return fl_presence

# contagem de produzidos por equipamento
def counter_nps_calc(id_equipamento):
    table = select_database_nps(id_equipamento)
    fl_presence = flag_presence_nps(table, id_equipamento)
    counter_db = len(table)

    if fl_presence:
        result = counter_db - 1
    else:
        result = counter_db
    return result

# soma a quantidade de produzidos dos equipamentos 82 e 89
def process_final():
    final82 = counter_nps_calc(82)
    final89 = counter_nps_calc(89)
    
    result = final82 + final89

    return result

    