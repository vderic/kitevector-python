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
embedding vector(3)
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
embedding   vector(3)
) server kite_svr options (table_name 'vector/vector*.csv', fmt 'csv', mpp_execute 'multi servers');
```


Run test,

```
% python3 test/test.py
```

To get the N-Best documents,

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
		rows, scores = vs.filter(kv.ScalarArrayOpExpr('id', [999, 4833])).limit(nbest).execute()
		print(rows)
		print(scores)
	except Exception as msg:
		print(msg)
```
