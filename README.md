# Spark JdbcRDD

## Load database data into Spark using Spark JdbcRDD using DataFrame API
(LoadDbToSparkRDD.java)
An RDD that executes an SQL query on a JDBC connection and reads results. JdbcRDD which can be used to load data from any JDBC compatible database
org.apache.spark.rdd.JdbcRDD<T> is asubclass of org.apache.spark.rdd.RDD<T> which means once we loaded DB data as JdbcRDD, all Spark operations can be applied on it. 

**JdbcRDD constructor has following parameters:**
```
JdbcRDD(
SparkContext sc, 
scala.Function0<java.sql.Connection> getConnection, 
String sql, 
long lowerBound, 
long upperBound, 
int numPartitions, 
scala.Function1<java.sql.ResultSet,T> mapRow, 
scala.reflect.ClassTag<T> evidence$1)
```

1. **SparkContext** sc: Spark context
2. **scala.Function0<java.sql.Connection> getConnection:**
This is Scala interface Function0<R> which doesn’t take any parameter and returns a result of type R (in this case, it is a java.sql.Connection). This is how database connection is supplied to JdbcRDD. Please note that this is Not an active DB connection. Functional programming technique is used here. This parameter supplies a function which knows how to acquire a DB connection but actual connection is made at a later stage when Spark decides it (upon invoking a RDD action).
3. **String sql:**
The SQL statement used to query the data from the database. This SQL must have 2 binding parameters of numeric types. Spark is a distributed system. To take advantage of it, the data has to be divided into multiple partitions (In cluster, these partitions will be in multiple Spark nodes). These binding parameters are used to query the data for a single partition. Next 3 parameters are used to determine the actual values of these parameters for each partition.
4. **long lowerBound:** Lower boundary of entire data
5. **long upperBound:** Upper boundary of entire data
6. **int numPartitions:** Number of partitions
7. **scala.Function1<java.sql.ResultSet,T> mapRow**
   This function is used to transform the returned ResultSet into a type of our choice.
   This is Scala interface Function1<T1, R> which takes a parameter of type T1 and returns result of type R. In this case, T1 is java.sql.ResultSet. R depends on what we want to transform it into.
8. **scala.reflect.ClassTag<T> evidence$1:** provide it as an instance of ClassTag.

##Framework
1. First  define the “getConnection” function for **scala.Function0<java.sql.Connection>** parameter which is an abstract class which implements Scala interface Function0. 
2. Create DbConnection class which then extends it and implements a **apply()** function by returning Connection object
3. Next define a **mapRow** function for **scala.Function1<java.sql.ResultSet,T>**. Here the ResultSet is converted to Object array using JdbcRDD utility function **resultSetToObjectArray**
4. Next instantiate the DbConnection object  with Db parameters defined as Java constants.
5. Finally instantiate the JdbcRDD


#Loading database data into Spark using Data Sources API
(LoadDbToSparkWithDataSource.java)

Data sources API which provides a unified interface to query external data sources from Spark SQL is introduced in Spark 1.2. However an official JDBC data source API is released only in version 1.3. This release also introduced a very important feature of Spark – The DataFrame API which is a higher level abstraction over RDD and provides a simpler API for data processing.

There are two ways to call data sources API

1. Programmatically using SQLContext load function
2. Using SQL 

**SQLContext load**
The load function is defined in Scala as follows,
```
def load(source: String, options: java.util.Map[String, String]): DataFrame
```

First parameter ‘source’ specifies the type of data source API. In our case, it is ‘jdbc’. 
2nd parameter is a map of options required by the data source implementation specified in the first parameter. 
It varies from data source to data source. 

**For JDBC datasource following parameters are required (Reproduced from Spark SQL documentation):**

**url:** The JDBC URL to connect to.
**dbtable:**
The JDBC table that should be read. Note that anything that is valid in a ‘FROM’ clause of a SQL query can be used. 
For example, instead of a full table you could also use a subquery in parentheses.
**driver:**
The class name of the JDBC driver needed to connect to this URL. This class with be loaded on the master and workers before running an JDBC commands to allow the driver to register itself with the JDBC subsystem.
**partitionColumn, lowerBound, upperBound, numPartitions:**
These options must all be specified if any of them is specified. They describe how to partition the table when reading in parallel from multiple workers. partitionColumn must be a numeric column from the table in question.