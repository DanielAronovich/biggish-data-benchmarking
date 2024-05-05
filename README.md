# biggish-data-benchmarking
A benchmarking project to compare the performance and usability of various data processing tools on a single machine:

1. Pandas 
2. DuckDB  
3. SQLite 
4. Dask
5. Modin
6. Vaex
7. Ray
8. Spark
9. cuDF
10. Polars

# Benchmarking standard

As benchmarking is fillied with many pitfalls, we will try to mitigate them by following  the recommendations from this paper:

[Raasveldt, Mark, et al. "Fair benchmarking considered difficult: Common pitfalls in database performance testing." Proceedings of the Workshop on Testing Database Systems. 2018.
](https://t1mm3.github.io/assets/papers/dbtest18.pdf)



## A. Getting started

After creating a venv for the project run:

```
pip install -r requirements.txt
```


### B. Generate input data tables

Run the bash script that first generates the tpch data tables in csv format approx 1Gb in size (scale factor 1), then it converts it to parquet files
```
./populate_data.sh 1
```

This will creat a folder named tables_scale_1 at the root folder of the project. 
