from cassandra.cluster import Cluster

class cassandra_conexao:
    
    def execute_query(query_text):
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect()
        session.execute('use thingsboard; ')
        query = session.execute(query_text)
        
        cluster.shutdown()
        
        return query 
