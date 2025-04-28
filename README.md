# my-apache-iceberg

When dealing with your data lake, its more efficient to store some metadata about the existing (parquet) files,
instead of reading all of them and then filtering the data. 

This goal of this project is to better understand the underworking of the Open Table Format and Apache Iceberg,
by trying to build a simple engine that would create metadata and use it for efficient querying of data.



What is Apache Iceberg?
> Adds data warehouse features on top of your data lake.
> Tracks schema evolution => Changes on the schema. Stores a schema for each snapshot.
> Partition evolution => Describe how partitioning changes.