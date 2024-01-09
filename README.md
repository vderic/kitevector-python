Install lz4, pandas, numpy package before install kite client,

```
% pip3 install pandas
% pip3 install numpy
% pip3 intall lz4
```

Install kite client,

```
% git clone git@github.com:vderic/kite-client-sdk.git
% cd kite-client-sdk/python
% pip3 install .
```

Install Kite Vector Store,

```
% git clone git@github.com:vderic/kitevector-python.git
% cd kitevector-python
% pip3 install .
```

Default Schema in Kite:
```
schema =  [('id', 'int64'), ('docid', 'int64'), ('embedding', 'float[]', 0, 0)]
```

Schema in PostgreSQL:
```
create extension vector;
create extension kite_fdw;
create server kite_svr FOREIGN DATA WRAPPER kite_fdw
OPTIONS (host '127.0.0.1:7878', port '5432', dbname 'pgsql', fragcnt '3', extensions 'vector');
DROP FOREIGN TABLE IF EXISTS ai_ext;
CREATE FOREIGN TABLE ai_ext (
id bigint,
docid bigint,
embedding vector(1536)
) server kite_svr options (schema_name 'public', table_name 'vector/vector*.csv', fmt 'csv');
```

Schema in GPDB:
```
create extension vector;
create extension kite_fdw;
create server kite_svr FOREIGN DATA WRAPPER kite_fdw
OPTIONS (host '127.0.0.1:7878', extensions 'vector');
DROP EXTERNAL TABLE IF EXISTS ai_ext;
CREATE FOREIGN TABLE ai_ext (
id bigint,
docid bigint,
embedding   vector(1536)
) server kite_svr options (table_name 'vector/vector*.csv', fmt 'csv', mpp_execute 'multi servers');
```


Run test,

```
% python3 test/test.py
```

To get the N-Best documents without Index,

```
	path = 'vector/vector*.csv'
	# use parquet as source file
	parquetspec = kite.ParquetFileSpec()
	hosts = ['localhost:7878']
	schema =  [('id', 'int64'), ('docid', 'int64'), ('embedding', 'float[]', 0, 0)]
	embedding = gen_embedding(1536)   # open AI embedding
	nbest = 3

	try:
		vs = kv.KiteVector(schema, hosts, fragcnt)
		vs.format(parquetspec).table(path).select(['id', 'docid']).order_by(kv.VectorExpr('embedding').inner_product(embedding))
		rows = vs.filter(kv.ScalarArrayOpExpr('id', [999, 4833])).limit(nbest).execute()
		print(rows)
	except Exception as msg:
		print(msg)
```

To get the N-Best documents Index,

```
	kite_hosts = ['localhost:7878']
	idx_hosts = ['localhost:8181']
	path = 'vector/vector*.csv'
	# use parquet as source file
	parquetspec = kite.ParquetFileSpec()
	schema =  [('id', 'int64'), ('docid', 'int64'), ('embedding', 'float[]', 0, 0)]
	embedding = gen_embedding(1536)   # open AI embedding
	nbest = 3

	# index specific setting
	space = 'ip'
	dim = 1536
	M = 16
	ef_construction = 200
	max_elements = 10000
	ef = 50
	num_threads = 1
	k = 10
	id_col = "id"     # column name of index column
	embedding_col = "embedding"   # column name of embedding column
	idxname = 'index'    # index name and idenitifier

	config = client.IndexConfig(idxname, space, dim, M, ef_construction,
		max_elements, ef, num_threads, k, id_col, embedding_col)

	try:
		vs = kv.KiteVector(schema, kite_hosts, fragcnt)
		vs.format(parquetspec).table(path).select(['id', 'docid']).order_by(kv.VectorExpr('embedding').inner_product(embedding)).limit(nbest)
		vs.index(idx_hosts, config)
		rows = vs.execute()
		print(rows)
	except Exception as msg:
		print(msg)
```

You can also generate the SQL and run with your favorite postgres client,

```
	embedding = gen_embedding(1536)   # open AI embedding
	nbest = 3
	table = 'ai_ext'

	try:
		vs = kv.PgVector()
		vs.table(table).select(['id', 'docid']).order_by(kv.VectorExpr('embedding').inner_product(embedding))
		sql = vs.filter(kv.ScalarArrayOpExpr('id', [999, 4833])).limit(nbest).sql()
		print(sql)
	except Exception as msg:
		print(msg)
```

