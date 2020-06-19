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

ToDo

### Query (Global Secondary Index)

ToDo

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

ToDo

### Scan

ToDo

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

ToDo

### Delete

ToDo

### Update

ToDo
