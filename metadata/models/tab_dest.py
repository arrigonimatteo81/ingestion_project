class TabDest:
    def __init__(self, id_dest, dest_name, dest_type, jdbc_url,
                 gcp_project, gcp_dataset, bucket_path, table_name,
                 user, password, extra_params):
        self.id_dest = id_dest
        self.dest_name = dest_name
        self.dest_type = dest_type
        self.jdbc_url = jdbc_url
        self.gcp_project = gcp_project
        self.gcp_dataset = gcp_dataset
        self.bucket_path = bucket_path
        self.table_name = table_name
        self.user = user
        self.password = password
        self.extra_params = extra_params
