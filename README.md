# Juttle Elastic Backend

The Juttle Elastic Backend enables reading and writing documents using [Elasticsearch](https://www.elastic.co/products/elasticsearch). It supports the [Logstash](https://www.elastic.co/products/logstash) schema, so it can read any documents stored in Elasticsearch by Logstash.

## Installation

In your Juttle repo, execute:
```
npm install juttle-elastic-backend
```

## Configuration

The information in the Juttle repository documentation under `configuration` and `Configuring backends` will help setup a general Juttle Config.

Configuration for the Elastic backend looks like this:
```
{
    "backends": {
        "elastic": {
            "address": "localhost",
            "port": 9200
        }
    }
}
```

To connect to an Elasticsearch instance elsewhere, add the appropriate address and port to this configuration.

## Usage

Here's a simple read command:
```
readx elastic -from :1 hour ago: -to :now:
```

This will output all points stored in Elastic timestamped with the last hour.

Here's a write:
```
emit -limit 1 | put name="test" | writex elastic
```

That will write a point timestamped with the current time, with one field `{name: "test"}`, which you'll then be able to query using `readx elastic`.

## Development

To run unit tests:
```
npm test
```

This is run automatically by Travis.
