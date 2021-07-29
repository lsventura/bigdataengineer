from datetime import datetime as dtime
import datetime as dt

class cassandra_queries:
        
    # Metodo para contagem de numProd 
    def select_database_nps(self, id_equipamento):
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

        query_production = \
            'SELECT valor ' \
            'FROM streamsets_line_h ' \
            'WHERE id_equipamento = ' + str(id_equipamento) + ' ' \
            'AND nome_sinal = \'NumProd\' ' \
            'AND created_at >= ' + str(filter_dat[0]) + ' ' \
            'AND created_at < ' +str(filter_dat[1]) + ' ALLOW FILTERING ;'

        query_nps = self.execute_query(query_production)

        num_prods = set()   

        if query_nps.one() is not None:
            for line in query_nps.current_rows:
                num_prods.add(line.valor)
                
        result = len(num_prods)

        return result

    # Metodo para verificar se há algum equipamento na esteira de produção
    def select_presence_product(self, id_equipamento):
        query_created_at =  \
            'SELECT max(created_at) as max_timestamp ' \
            'FROM streamsets_line_h ' \
            'WHERE  nome_sinal = \'PresencaProduto\' ' \
            'AND id_equipamento = '+ str(id_equipamento) + ' ALLOW FILTERING ;'
    
        query_max_created_at = self.execute_query(query_created_at)
        result = None
        max_created_at = query_max_created_at.one().max_timestamp
        
        if max_created_at is not None:
        
            query_valor =  \
            'SELECT valor ' \
            'FROM streamsets_line_h ' \
            'WHERE  nome_sinal = \'PresencaProduto\' ' \
            'AND  created_at = ' + str(max_created_at) + ' ' \
            'AND id_equipamento = '+ str(id_equipamento) + ' ALLOW FILTERING ;'
            
            query_presence = self.execute_query(query_valor)
            
            result = int(query_presence.one().valor)

        return result
