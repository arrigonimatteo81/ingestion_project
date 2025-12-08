CREATE TABLE tab_conn (
    id_conn SERIAL PRIMARY KEY,
    conn_name VARCHAR(100),
    conn_type VARCHAR(50), -- jdbc, csv, driver
    jdbc_url TEXT,
    host TEXT,
    port INT,
    db_name TEXT,
    user TEXT,
    password TEXT,
    extra_params JSONB
);

CREATE TABLE tab_dest (
    id_dest SERIAL PRIMARY KEY,
    dest_name VARCHAR(100),
    dest_type VARCHAR(50), -- jdbc, bigquery, file
    jdbc_url TEXT,
    gcp_project TEXT,
    gcp_dataset TEXT,
    bucket_path TEXT,
    table_name TEXT,
    user TEXT,
    password TEXT,
    extra_params JSONB
);

CREATE TABLE tab_src (
    id_src SERIAL PRIMARY KEY,
    conn_id INT REFERENCES tab_conn(id_conn),
    id_dest INT REFERENCES tab_dest(id_dest),
    src_type VARCHAR(50),
    src_table TEXT,
    src_query TEXT,
    file_path TEXT
);

--utente utente2025