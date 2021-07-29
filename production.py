class producao:
    def counter_nps_calc(self, id_equipamento):
        counter_db = self.select_database_nps(id_equipamento)
        fl_presence = self.select_presence_product(id_equipamento)
    
        if fl_presence:
            result = counter_db - 1
        else:
            result = counter_db
        return result

    # soma a quantidade de produzidos dos equipamentos 82 e 89
    def process_final(self):
        qtd_82 = self.counter_nps_calc(82)
        qtd_89 = self.counter_nps_calc(89)
        
        result = qtd_82 + qtd_89

        return result
