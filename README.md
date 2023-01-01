<p align="center">
  <img src="https://user-images.githubusercontent.com/7671010/209962006-fa72b231-fb12-4e78-8c72-eb7906874650.png" height="70">
</p>


dbREST is basically an API backend that you can put in front of your database. Ever wanted to spin up an API service in front of your Snowflake, MySQL or even SQLite database? Well, dbREST allows that! See https://docs.dbrest.io for more details.

Running `dbrest serve` will launch an API process which allow you to:

  
<details><summary>Select a table's data</summary>
  
```http
GET /snowflake_db/my_schema/docker_logs?fields=container_name,timestamp&limit=100
```
  
```json
[
  { "container_name": "vector", "timestamp": "2022-04-22T23:54:06.644268688Z" },
  { "container_name": "postgres", "timestamp": "2022-04-22T23:54:06.644315426Z" },
  { "container_name": "api", "timestamp": "2022-04-22T23:54:06.654821046Z" },
]
```
</details>
  
<details><summary>Insert into a table</summary>
  
```http
POST /snowflake_db/my_schema/docker_logs

[
  {"container_name":"vector","host":"vector","image":"timberio/vector:0.21.1-debian","message":"2022-04-22T23:54:06.644214Z  INFO vector::sources::docker_logs: Capturing logs from now on. now=2022-04-22T23:54:06.644150817+00:00","stream":"stderr","timestamp":"2022-04-22T23:54:06.644268688Z"}
]
```
</details>
  
<details><summary>Update a table</summary>
  
```http
PATCH /snowflake_db/my_schema/my_table?key=col1

[
  { "col1": "123", "timestamp": "2022-04-22T23:54:06.644268688Z" },
  { "col1": "124", "timestamp": "2022-04-22T23:54:06.644315426Z" },
  { "col1": "125", "timestamp": "2022-04-22T23:54:06.654821046Z" }
]
```
</details>
  
<details><summary>Upsert into a table</summary>
  
```http
PUT /snowflake_db/my_schema/my_table?key=col1

[
  { "col1": "123", "timestamp": "2022-04-22T23:54:06.644268688Z" },
  { "col1": "124", "timestamp": "2022-04-22T23:54:06.644315426Z" },
  { "col1": "125", "timestamp": "2022-04-22T23:54:06.654821046Z" }
]
```
</details>
  
<details><summary>Submit a Custom SQL query</summary>
  
```http
POST /snowflake_db/.sql

select * from my_schema.docker_logs where timestamp is not null
```
  
```json
[
  { "container_name": "vector", "timestamp": "2022-04-22T23:54:06.644268688Z" },
  { "container_name": "postgres", "timestamp": "2022-04-22T23:54:06.644315426Z" },
  { "container_name": "api", "timestamp": "2022-04-22T23:54:06.654821046Z" },
]
```
</details>
  
<details><summary>List all columns in a table</summary>
  
```http
GET /snowflake_db/my_schema/docker_logs/.columns
```
  
```json
[
  {"column_id":1,"column_name":"timestamp", "column_type":"String", "database_name":"default", "schema_name":"my_schema", "table_name":"docker_logs", "table_type":"table"},
  {"column_id":2,"column_name":"container_name", "column_type":"String", "database_name":"default", "schema_name":"my_schema", "table_name":"docker_logs", "table_type":"table"},
  {"column_id":3,"column_name":"host", "column_type":"String", "database_name":"default", "schema_name":"my_schema", "table_name":"docker_logs", "table_type":"table"},{"column_id":4,"column_name":"image", "column_type":"String", "database_name":"default", "schema_name":"my_schema", "table_name":"docker_logs", "table_type":"table"},
]
```
</details>
  
<details><summary>List all tables in a schema</summary>
  
```http
GET /snowflake_db/my_schema/.tables
```
  
```json
[
  {"database_name":"default", "is_view":"table", "schema_name":"my_schema", "table_name":"docker_logs"},
  {"database_name":"default", "is_view":"table", "schema_name":"my_schema", "table_name":"example"},
  {"database_name":"default", "is_view":"view", "schema_name":"my_schema", "table_name":"place_vw"}
]
```
</details>
  
  
<details><summary>List all columns, in all tables in a schema</summary>
  
```http
GET /snowflake_db/my_schema/.columns
```
  
```json
[
  {"column_id":1,"column_name":"timestamp", "column_type":"String", "database_name":"default", "schema_name":"my_schema", "table_name":"docker_logs", "table_type":"table"},
  {"column_id":2,"column_name":"container_name", "column_type":"String", "database_name":"default", "schema_name":"my_schema", "table_name":"docker_logs", "table_type":"table"},
  {"column_id":3,"column_name":"host", "column_type":"String", "database_name":"default", "schema_name":"my_schema", "table_name":"docker_logs", "table_type":"table"},{"column_id":4,"column_name":"image", "column_type":"String", "database_name":"default", "schema_name":"my_schema", "table_name":"docker_logs", "table_type":"table"},
]
```
</details>

Of course there must be an authentication / authorization logic. It is based on tokens being issued with the `dbrest token` sub-command which are tied to roles defined in a YAML config file:

```yaml
reader:
  snowflake_db:
    allow_read:
      - schema1.*
      - schema2.table1
    allow_sql: 'disable'

  my_pg:
    allow_read:
      - '*'
    allow_sql: 'disable' 

writer:
  snowflake_db:
    allow_read:
      - schema1.*
      - schema2.table1
    allow_write:
      - schema2.table3
    allow_sql: 'disable'

  my_pg:
    allow_read:
      - '*'
    allow_write:
      - '*'
    allow_sql: 'any' 
```

We can now issue tokens with `dbrest tokens issue <token_name> --roles reader,writer`.
  
It is built in Go. And as you might have guessed, it also powers alot of [`dbNet`](https://github.com/dbnet-io/dbnet) :).

dbREST is in active developement. Here are some of the databases it connects to:
* Clickhouse
* Google BigQuery
* Google BigTable
* MySQL
* Oracle
* Redshift
* PostgreSQL
* SQLite
* SQL Server
* Snowflake
* DuckDB (coming soon)
* ScyllaDB (coming soon)
* Firebolt (coming soon)
* Databricks (coming soon)

# Running it locally

## Brew (Mac)

```bash
brew install dbrest-io/dbrest/dbrest

# You're good to go!
dbrest -h
```
## Scoop (Windows)

```bash
scoop bucket add org https://github.com/dbrest-io/scoop-dbrest.git
scoop install dbrest

# You're good to go!
dbrest -h
```
## Docker

```bash
docker run --rm -it dbrest/dbrest -h
```

## Binary (Linux)

```bash
# Download binary (amd64)
curl -LO 'https://github.com/dbrest-io/dbREST/releases/latest/download/dbrest_linux_amd64.tar.gz' \
  && tar xf dbrest_linux_amd64.tar.gz \
  && rm -f dbrest_linux_amd64.tar.gz \
  && chmod +x dbrest

# You're good to go!
./dbrest -h
```

## From Source

```bash
git clone https://github.com/dbrest-io/dbREST.git
cd dbREST
go mod tidy # get all dependencies
go build -o dbrest
```