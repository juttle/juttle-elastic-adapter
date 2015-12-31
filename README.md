# Juttle Elastic Adapter

[![Build Status](https://travis-ci.org/juttle/juttle-elastic-adapter.svg)](https://travis-ci.org/juttle/juttle-elastic-adapter)

The Juttle Elastic Adapter enables reading and writing documents using [Elasticsearch](https://www.elastic.co/products/elasticsearch). It supports the [Logstash](https://www.elastic.co/products/logstash) schema, so it can read any documents stored in Elasticsearch by Logstash.

## Examples

Read all documents stored in Elasticsearch timestamped with the last hour:

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

The value for `juttle-elastic-adapter` can also be an array of Elasticsearch host locations. Give each one a unique `id` field, and `read -id` and `write -id` will use the appropriate host.

The Juttle Elastic Adapter can also make requests to Amazon Elasticsearch Service instances, which requires a little more configuration. To connect to Amazon Elasticsearch Service, an entry in the `juttle-elastic-adapter` config must have `{"type": "aws"}` as well as `"region"`, `"endpoint"`, `"access_key"`, and `"secret_key"` fields. `"access_key"` and `"secret_key"` can also be specified by the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` respectively.

Here's an example Juttle Elastic Adapter configuration that can read from either a local Elasticsearch instance running on port 9200 or an Amazon Elasticsearch Service at `search-foo-bar.us-west-2.es.amazonaws.com`:

```json
{
    "adapters": {
        "juttle-elastic-adapter": [
            {
                "id": "local",
                "address": "localhost",
                "port": 9200
            },
            {
                "id": "amazon",
                "type": "aws",
                "endpoint": "search-foo-bar.us-west-2.es.amazonaws.com",
                "region": "us-west-2",
                "access_key": "(my access key ID)",
                "secret_key": "(my secret key)"
            }
        ]
    }
}
```

Then `read -id "amazon"` will return points stored in the Amazon Elasticsearch Service instance search-foo-bar.us-west-2.es.amazonaws.com (provided the access_key and secret_key are those of an account with authorization for it), and `read -id "local"` will return points stored in the Elasticsearch instance at localhost:9200.

## Schema ##
To read or write data, the adapter has to know the names of the indices storing that data in Elasticsearch. By default, the adapter assumes that it is dealing with data written by Logstash using Logstash's default configuration: that is, an index per day with name format `logstash-${yyyy.mm.dd}`.

The schema used by the adapter is configurable via options to read, and you can modify the defaults in configuration. The prefix `logstash-` can be replaced by an arbitrary string with the `index_prefix` option, and the timespan of each index can be modified by the `index_interval` option. Valid values for `index_interval` are `"day"`, `"week"`, `"month"`, `"year"`, and `"none"`. With `"week"`, the adapter will use indices formatted `${prefix}${yyyy.ww}`, where `ww` ranges from 01 to 53 numbering the weeks in a year. With `"month"`, it will use `${prefix}${yyyy.mm}`, and with `"year"`, it will use `${prefix}${yyyy}`. With `"none"`, it will use just one index entirely specified by `index_prefix`.

Also, the adapter expects all documents in Elasticsearch to have a field containing a timestamp. By default, it expects this to be the `@timestamp` field. This is configurable with the `-time_field` option to `read` and `write`.

## Usage

### Read options


Name | Type | Required | Description | Default
-----|------|----------|-------------|---------
`from` | moment | no | select points after this time (inclusive) | none, either `-from` and `-to` or `-last` must be specified
`to`   | moment | no | select points before this time (exclusive) | none, either `-from` and `-to` or `-last` must be specified
`last` | duration | no | select points within this time in the past (exclusive) | none, either `-from` and `-to` or `-last` must be specified
`id` | string | no | read from the configured Elasticsearch endpoint with this ID | the first endpoint in `config.json`
`index_prefix` | string | no | read from indices whose names start with this string | `logstash-`
`index_interval` | string | no | read from indices that have this granularity. valid options: `"day"`, `"week"`, `"month"`, `"year"`, `"none"` | `day`
`time_field` | string | no | field containing timestamps | `@timestamp`

### Write options

Name | Type | Required | Description | Default
-----|------|----------|-------------|---------
`id` | string | no | write to the configured Elasticsearch endpoint with this ID | the first endpoint in `config.json`
`index_prefix` | string | no | write to indices whose names start with this string | `logstash-`
`index_interval` | string | no | write to indices that have this granularity. valid options: `"day"` `"week"`, `"month"`, `"year"`, `"none"` | `day`
`time_field` | string | no | field containing timestamps | `@timestamp`

## Contributing

Want to contribute? Awesome! Don’t hesitate to file an issue or open a pull
request.
