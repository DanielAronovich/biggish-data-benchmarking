# biggish-data-benchmarking
A benchmarking project to compare the performance and usability of various data processing tools like Pandas, DuckDB, SQLite, Dask, Modin, and Vaex on a single laptop


## A. Getting started





### B. Generate input data tables

Navigate to the data generator directory `dbgen` and build the data generator:

```
cd dbgen
make
```

This should generate an executable called `dbgen`. Use the `-h` flag to see the various options the tool offers.

```
./dbgen -h
```

The simplest case is running the `dbgen` executable with no flags.

```
./dbgen
```

The above generates tables with extension `.tbl` with scale 1 (default) for a total of roughly 1GB size across all tables.

```bash
$ ls -hl *.tbl
-rw-rw-r-- 1 savvas savvas  24M May 28 12:39 customer.tbl
-rw-rw-r-- 1 savvas savvas 725M May 28 12:39 lineitem.tbl
-rw-rw-r-- 1 savvas savvas 2.2K May 28 12:39 nation.tbl
-rw-rw-r-- 1 savvas savvas 164M May 28 12:39 orders.tbl
-rw-rw-r-- 1 savvas savvas 114M May 28 12:39 partsupp.tbl
-rw-rw-r-- 1 savvas savvas  24M May 28 12:39 part.tbl
-rw-rw-r-- 1 savvas savvas  389 May 28 12:39 region.tbl
-rw-rw-r-- 1 savvas savvas 1.4M May 28 12:39 supplier.tbl
```

For different size tables you can use the `-s` (scale) option. For example,

```
./dbgen -s 10
```

will generate roughly 10GB of input data.

Note that by default, `dbgen` uses a `|` as a column separator, and includes a `|` at the end of each entry.

```bash
$ cat region.tbl 
0|AFRICA|lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to |
1|AMERICA|hs use ironic, even requests. s|
2|ASIA|ges. thinly even pinto beans ca|
3|EUROPE|ly final courts cajole furiously final excuse|
4|MIDDLE EAST|uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl|
```
