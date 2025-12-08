class TabConn:
    def __init__(self, id_conn, conn_name, conn_type, jdbc_url, host, port,
                 db_name, user, password, extra_params):
        self.id_conn = id_conn
        self.conn_name = conn_name
        self.conn_type = conn_type
        self.jdbc_url = jdbc_url
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.extra_params = extra_params
