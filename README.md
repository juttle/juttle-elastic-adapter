# Juttle Elastic Adapter

The Juttle Elastic Adapter enables reading and writing documents using [Elasticsearch](https://www.elastic.co/products/elasticsearch). It supports the [Logstash](https://www.elastic.co/products/logstash) schema, so it can read any documents stored in Elasticsearch by Logstash.

## Examples

Read all documents stored in Elastic timestamped with the last hour:

```juttle
read elastic -from :1 hour ago: -to :now:
```

Write a document timestamped with the current time, with one field `{ name: "test" }`, which you'll then be able to query using `read elastic`.

```juttle
emit -limit 1 | put name="test" | write elastic
```

## Installation

Like Juttle itself, the adapter is installed as a npm package. Both Juttle and
the adapter need to be installed side-by-side:

```bash
$ npm install juttle
$ npm install juttle-elastic-adapter
```

## Configuration

The adapter needs to be registered and configured so that it can be used from
within Juttle. To do so, add the following to your `~/.juttle/config.json` file:

```json
{
    "adapters": {
        "juttle-elastic-adapter": {
            "address": "localhost",
            "port": 9200
        }
    }
}
```

To connect to an Elasticsearch instance elsewhere, change the `address`
and `port` in this configuration.

## Usage

### Read options


Name | Type | Required | Description
-----|------|----------|-------------
`from` | moment | no | select points after this time (inclusive)
`to`   | moment | no | select points before this time (exclusive)
`last` | duration | no | select points within this time in the past (exclusive)

### Write options

Name | Type | Required | Description
-----|------|----------|-------------

## Contributing

Want to contribute? Awesome! Don’t hesitate to file an issue or open a pull
request.
