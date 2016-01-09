# Juttle Elastic Adapter

[![Build Status](https://travis-ci.org/juttle/juttle-elastic-adapter.svg)](https://travis-ci.org/juttle/juttle-elastic-adapter)

The Juttle Elastic Adapter enables reading and writing documents using [Elasticsearch](https://www.elastic.co/products/elasticsearch).

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

The Juttle Elastic Adapter can also make requests to Amazon Elasticsearch Service instances, which requires a little more configuration. To connect to Amazon Elasticsearch Service, an entry in the `juttle-elastic-adapter` config must have `{"type": "aws"}` as well as `region`, `endpoint`, `access_key`, and `secret_key` fields. `access_key` and `secret_key` can also be specified by the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` respectively.

Here's an example Juttle Elastic Adapter configuration that can connect to a local Elasticsearch instance running on port 9200 using `read/write elastic -id "local"` and an Amazon Elasticsearch Service at `search-foo-bar.us-west-2.es.amazonaws.com` using `read/write elastic -id "amazon"`:

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

## Schema ##
To read or write data, the adapter has to know the names of the indices storing that data in Elasticsearch. By default, the adapter writes points to an index called `juttle` and reads from all indices.

You can choose indices to read and write from with the `-index` option, or you can specify an `index` for each configured Elasticsearch instance the adapter is connected to.

For schemas such as [Logstash](https://www.elastic.co/products/logstash) that create indices at regular intervals, the adapter supports an `indexInterval` option. Valid values for `indexInterval` are `day`, `week`, `month`, `year`, and `none`. With `week`, the adapter will use indices formatted `${index}${yyyy.ww}`, where `ww` ranges from 01 to 53 numbering the weeks in a year. With `month`, it will use `${index}${yyyy.mm}`, and with `year`, it will use `${index}${yyyy}`. With `none`, the default, it will use just one index entirely specified by `index`. When using `indexInterval`, `index` should be the non-date portion of each index followed by `*`.

Lastly, the adapter expects all documents in Elasticsearch to have a field containing a timestamp. By default, it expects this to be the `@timestamp` field. This is configurable with the `-timeField` option to `read` and `write`.

#### Logstash ####
Let's look at Logstash for an example of configuring a schema. Logstash creates daily indices that look like `logstash-2016.01.05`. By default, the adapter reads from all indices, so if you want to read from only Logstash's indices, use `logstash-*` for the `index` option.

If you have many days' worth of data, Logstash will create many indices. Reading from many indices can be slow in Elasticsearch. To speed things up in this case, `read elastic` can narrow its search to only the days it needs for its query. To get this behavior, use `-indexInterval "day"`. Also, `write elastic` requires `-indexInterval "day"` for writing into daily indices.

## Usage

### Read options


Name | Type | Required | Description | Default
-----|------|----------|-------------|---------
`from` | moment | no | select points after this time (inclusive) | none, either `-from` and `-to` or `-last` must be specified
`to`   | moment | no | select points before this time (exclusive) | none, either `-from` and `-to` or `-last` must be specified
`last` | duration | no | select points within this time in the past (exclusive) | none, either `-from` and `-to` or `-last` must be specified
`id` | string | no | read from the configured Elasticsearch endpoint with this ID | the first endpoint in `config.json`
`index` | string | no | index(es) to read from | `*`
`indexInterval` | string | no | granularity of an index. valid options: `day`, `week`, `month`, `year`, `none` | `none`
`type` | string | no | [document type](https://www.elastic.co/guide/en/elasticsearch/guide/current/mapping.html) to read from | all types
`timeField` | string | no | field containing timestamps | `@timestamp`
`idField` | string | no | if specified, the value of this field in each point emitted by `read elastic` will be the [document ID](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-id-field.html) of the corresponding Elasticsearch document | none

### Write options

Name | Type | Required | Description | Default
-----|------|----------|-------------|---------
`id` | string | no | write to the configured Elasticsearch endpoint with this ID | the first endpoint in `config.json`
`index` | string | no | index to write to | `juttle`
`indexInterval` | string | no | granularity of an index. valid options: `day` `week`, `month`, `year`, `none` | `none`
`type` | string | no | document type to write to | `event`
`timeField` | string | no | field containing timestamps | `@timestamp`
`idField` | string | no | if specified, the value of this field on each point will be used as the document ID for the corresponding Elasticsearch document and not stored | none

## Contributing

Want to contribute? Awesome! Don’t hesitate to file an issue or open a pull
request.
