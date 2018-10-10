# orangutang 
# a tool for bigdata  integration


description：
1. Data source 
   1.1 SQL library supports all databases that can be linked through JDBC and odbc.
   1.2nosql library mainly supports spark- SQL, HQL, DSL and other data that can be queried.
   1.3 semi-structured data, json, XML, CSV, excel 
   1.4 analytical data, unstructured data import, such as support for parsing rules of entry 

2. The data processing layer support full table SQL database, SQL query results, and other data parsed to generate data flow, (you can set the batch number), 
   support automation to create table, or on the basis of the existing table, support data flow and the mapping relationship between disease field import. 

3. Data storage. At present, it is mainly output to relational database, distributed library ES, hive, hbase. 4. Submit workflow (including table source, data source, data flow, etc.)
