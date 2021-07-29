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
    h_finish = dt.time(5, 45, 00)

    d_yesterday = d_now - dt.timedelta(days=1)
    dt_yesterday = dtime.combine(d_yesterday, h_finish)

    d_tomorrow = d_now + dt.timedelta(days=1)
    dt_tomorrow = dtime.combine(d_tomorrow, h_finish)

    d_current = dtime.combine(d_now, h_finish)
    
    filter_dat = ''
    
    if h_now <= h_finish:
        filter_dat = [int((dtime.timestamp(dt_yesterday)) * 1000), int((dtime.timestamp(d_current)) * 1000)]
    
    elif h_now > h_finish:
        filter_dat = [int((dtime.timestamp(d_current)) * 1000), int((dtime.timestamp(dt_tomorrow)) * 1000)]

    query_text =  'SELECT valor ' \
                  'FROM streamsets_line_h ' \
                  'WHERE id_equipamento = ' + str(id_equipamento) + ' ' \
                  'AND nome_sinal = \'NumProd\' ' \
                  'AND created_at >= ' + str(filter_dat[0]) + ' ' \
                  'AND created_at < ' +str(filter_dat[1]) + ' ALLOW FILTERING ;'

    query = execute_query(query_text)

    num_prods = set()   

    if query.one() is not None:
        for line in query.current_rows:
            num_prods.add(line.valor)
            
    result = len(num_prods)

    return result

# Metodo para verificar se há algum equipamento na esteira de produção
def select_presence_product(id_equipamento):
    query_created_at =  \
        'SELECT max(created_at) as max_timestamp ' \
        'FROM streamsets_line_h ' \
        'WHERE  nome_sinal = \'PresencaProduto\' ' \
        'AND id_equipamento = '+ str(id_equipamento) + ' ALLOW FILTERING ;'
  
    query = execute_query(query_created_at)
    result = None
    max_created_at = query.one().max_timestamp
    
    if max_created_at is not None:
    
        query_valor =  \
        'SELECT valor ' \
        'FROM streamsets_line_h ' \
        'WHERE  nome_sinal = \'PresencaProduto\' ' \
        'AND  created_at = ' + str(max_created_at) + ' ' \
        'AND id_equipamento = '+ str(id_equipamento) + ' ALLOW FILTERING ;'
        
        query_presence = execute_query(query_valor)
        
        result = int(query_presence.one().valor)

    return result

#############################################################################################################################################
#### Arquivo productions.py
#############################################################################################################################################
# contagem de produzidos por equipamento
def counter_nps_calc(id_equipamento):
    counter_db = select_database_nps(id_equipamento)
    fl_presence = select_presence_product(id_equipamento)
 
    if fl_presence:
        result = counter_db - 1
    else:
        result = counter_db
    return result

# soma a quantidade de produzidos dos equipamentos 82 e 89
def process_final():
    qtd_82 = counter_nps_calc(82)
    qtd_89 = counter_nps_calc(89)
    
    result = qtd_82 + qtd_89

    return result

    