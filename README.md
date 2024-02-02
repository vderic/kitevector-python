Install lz4, pandas, numpy package before install kite client,

```
% pip3 install pandas
% pip3 install numpy
% pip3 intall lz4
% pip3 install hnswlib

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
) server kite_svr options (schema_name 'public', table_name 'vector/vector*.parquet', fmt 'parquet');
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
) server kite_svr options (table_name 'vector/vector*.parquet', fmt 'parquet', mpp_execute 'multi servers');
```


Run test,

```
% python3 test/test.py
```

To get the N-Best documents without Index,

```
	path = 'vector/vector*.parquet'
	# use parquet as source file
	parquetspec = kite.ParquetFileSpec()
	hosts = ['localhost:7878']
	schema = [{'name':'id', 'type':'int64'},
		{'name':'docid', 'type':'int64'},
		{'name':'embedding', 'type':'float[]'}]
	embedding = gen_embedding(1536)   # open AI embedding
	nbest = 3

	# Flat index specific setting
	flat_index_params = {
		"metric_type": "ip",
		"index_type": "flat",
		"params":{
			"id_field" : "id",
			"vector_field": "embedding"
		}
	}

	try:
		vs = kv.KiteVector(schema, hosts, fragcnt)
		vs.format(parquetspec).table(path).select(['docid']).index(flat_index_params, embedding)
		rows = vs.filter(kv.ScalarArrayOpExpr('id', [999, 4833])).limit(nbest).execute()
		print(rows)
	except Exception as msg:
		print(msg)
```

To get the N-Best documents with distributed index,

```
	kite_hosts = ['localhost:7878']
	idx_hosts = ['localhost:8878']
	path = 'vector/vector*.parquet'
	# use parquet as source file
	parquetspec = kite.ParquetFileSpec()
	schema = [{'name':'id', 'type':'int64'},
		{'name':'docid', 'type':'int64'},
		{'name':'embedding', 'type':'float[]'}]
	embedding = gen_embedding(1536)   # open AI embedding
	nbest = 3

	# index specific setting
	index_params = {
		"name" : "movie",
		"metric_type": "ip",
		"index_type": "hnsw",
		"params":{
			"dimension": 1536,
			"max_elements": 100,
			"M": 16,
			"ef_construction": 200,
			"ef" : 50,
			"num_threads": 1,
			"k" : 10,
			"id_field" : "id",
			"vector_field": "embedding"
		}
	}

	try:
		vs = kv.KiteVector(schema, kite_hosts, fragcnt)
		vs.format(parquetspec).table(path).select(['docid']).limit(nbest)
		vs.index(index_params, embedding, hosts=idx_hosts)
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

	# index specific setting
	index_params = {
		"metric_type": "ip",
		"index_type": "hnsw",
		"params":{
			"id_field" : "id",
			"vector_field": "embedding"
		}
	}

	try:
		vs = kv.PgVector()
		vs.table(table).select(['id', 'docid']).index(index_params, embedding)
		sql = vs.filter(kv.ScalarArrayOpExpr('id', [999, 4833])).limit(nbest).sql()
		print(sql)
	except Exception as msg:
		print(msg)
```

Note:

I tried to put one million vectors and the vector dimension was 1,280. The HNSW requires 4 * d + 8 * M Bytes for each vector for just storing the graph. The Amazon ES team recommended me to have 1.5 times larger memory than the required amount. The M was 48 in the experiment, so (4 * 1,280 + 8 * 48) * 1.5 * 1M = 7.7GB is needed for the dataset.
