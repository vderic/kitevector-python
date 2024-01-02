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
% git clone git@github.com:vderic/kitevs-python.git
% cd kitevs-python
% pip3 install .
```

Run test,

```
% python3 test/test.py
```

To get the N-Best documents,

```
	path = 'vector/vector*.csv'
	filespec = kite.CsvFileSpec()
	hosts = ['localhost:7878']
	embedding = [4,6,8]
	threshold = 0.8
	nbest = 3

	vs = kitevs.KiteVectorStore(hosts, path, filespec)
	res = vs.nbest(embedding, None, threshold, nbest)
	print(res)
```
