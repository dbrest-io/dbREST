# dbrest
Spin up a REST API for any Major Database

## Roles examples

```yaml

roles:
  reader:
    MY_SNOWFLAKE:
      allow_read:
        - schema1.*
        - schema2.table1
        - schema2.table2
      allow_sql: 'disable'

    MY_PG:
      allow_read:
        - '*'
      allow_sql: 'only_select' 
  writer:
    MY_SNOWFLAKE:
      allow_write:
        - schema2.table3

    MY_PG:
      allow_write:
        - '*'
      allow_sql: 'only_select'
```