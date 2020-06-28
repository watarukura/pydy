# pydy

DynamoDB Command Line Interface

## Installation

ToDo

## Usage

### Get

```sh
pydy get --table <TableName> \
    --pkey <PartitionKey> \
    --skey <SortKey>
```

### Query (PartitionKey, SortKey)

```sh
pydy query --table <TableName> \
    --pkey <PartitionKey> \
    --skey <SortKey> \
    --skey_cond <Condition(eq|ne|gt|ge|lt|le|begins_with|between|contains)>
```

### Query (Global Secondary Index)

```sh
pydy query --table <TableName> \
    --pkey <PartitionKey> \
    --skey <SortKey> \
    --skey_cond <Condition(eq|ne|gt|ge|lt|le|begins_with|between|contains)>
    --index <(Global|Local)SecondaryIndex>
```

### Put

```sh
pydy put --table <TableName> \
    --payload <Payload>
```

```sh
cat payload.json
{"Id": 1}
pydy put --table Sample --payload $(cat payload.json)
```

### Delete

```sh
pydy delete --table <TableName> \
    --pkey <PartitionKey> \
    --skey <SortKey>
```

### Scan

```sh
pydy scan --table <TableName>
    --filter_key <AttributeName>
    --filter_cond <FilteringCondition(eq|ne|gt|ge|lt|le|begins_with|between|contains)> \
    --filter_value <FilteringValue>
    --limit <Limit>
```

### Desc

```sh
pydy desc --table <TableName>
```

### List


```sh
pydy list
```

### Create

```sh
pydy create --ddl_json <DdlJSon>
```

### Drop

```
pydy drop --table <TableName>
```

### Update

```
pydy update --table <TableName> \
    --pkey <PartitionKey> \
    --skey <SortKey> \
    --update_attr <UpdateAttribute> \
    --update_value <UpdateValue>
```
