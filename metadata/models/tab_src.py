class TabSrc:
    def __init__(self, id_src, conn_id, id_dest, src_type,
                 src_table, src_query, file_path):
        self.id_src = id_src
        self.conn_id = conn_id
        self.id_dest = id_dest
        self.src_type = src_type
        self.src_table = src_table
        self.src_query = src_query
        self.file_path = file_path
