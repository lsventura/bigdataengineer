dataframeJson = [
   {
     "nome_sinal": "",
     "valor": "",
     "id_equipamento": 82,
     "created_at": 
    },
   {
     "nome_sinal": "",
     "valor": "",
     "id_equipamento": 82,
     "created_at": 
    },
   {
     "nome_sinal": "",
     "valor": "",
     "id_equipamento": 82,
     "created_at": 
    },
   {
     "nome_sinal": "",
     "valor": "",
     "id_equipamento": 82,
     "created_at": 
    },
   {
     "nome_sinal": "",
     "valor": "",
     "id_equipamento": 82,
     "created_at": 
    } ]


###### cassandra_queries.py

#Metodo para contagem de numProd 
#def select_contagem_nps(self, id_equipamento):
#   d_now = now.date(now().year, now().month, d_h_now()now.day)
#   h_now = now.time(now().hour, now().minute, now().second)
#   h_finish = now.time(17, 45, 00)
#   d_current = datetime.combine(d_now, h_finish)
#   d_yesterday = d_now - timedelta(days=1)
#   dt_yesterday = datetime.combine(d_yesterday, h_finish)
#
#   filter_dat = ''
#
#   if h_now <= h_finish:
#       filter_dat = int((datetime.timestamp(dt_yesterday)) * 1000)
#
#   elif h_now > h_finish:
#       filter_dat = int((datetime.timestamp(d_current)) * 1000)
#
#   return filter_dat
#
#   query_text =  \
#           'SELECT DISTINCT nome_sinal, valor, id_equipamento, created_at ' \
#           'FROM thingsboard.streamsets_line_h ' \
#           'WHERE id_equipamento = ' + id_equipamento  + ' ' \
#           'AND created_at >= ' + filter_dat + ';'
#   
#   query = self.cassandra.execute_query(query_text)
#   
#       if query.one() is not None:
#           result = []
#           for line in query.current_rows:
#               result.append({
#                   'nome_sinal': line.nome_sinal,
#                   'valor': line.valor,
#                   'id_equipamento': line.id_equipamento,
#                   'created_at': line.created_at
#               })
#
#       result.sort(key=lambda x: x['created_at'])
#
#   return result
#   
#   #Metodo para contagem de numProd 
#   def select_presence_product(self, id_equipamento):
#         query_text =  \
#           'SELECT max(created_at) max_timestamp ' \
#           'FROM thingsboard.streamsets_line_h ' \
#           'WHERE nome_sinal = \'PresencaProduto\ ' \
#           'AND id_equipamento in ('+ id_equipamento + ') ;'
#   
#   query = self.cassandra.execute_query(query_text)
#
#      if query.one() is not None:
#           result = []
#           for line in query.current_rows:
#               result.append({
#                   'max_timestamp': line.max_timestamp
#                   })
#
#   return result


###### production.py

    def flag_presence_nps(table, id_equipamento):
        max_timestamp = self.cassandra.select_presence_product(id_equipamento)

        for line in table:
            if table["created_at"] = max_timestamp:
                fl_presence = True
            else:
                fl_presence = False
                
        return fl_presence

    @staticmethod
    # contagem de numeros de producao
    def counter_nps_calc(id_equipamento):
        equip = self.cassandra.select_contagem_nps(id_equipamento)

        counter_equip = equip.count()
        
        fl_presence = flag_presence_nps(equip, id_equipamento)

        if fl_presence:
            counter_equip - 1
        
        return counter_equip

    def process_final():
        qtd_final82 = counter_nps_calc(82)
        qtd_final89 = counter_nps_calc(89)
        
        nps = qtd_final82 + qtd_final89

        return nps