# Juttle Elastic Adapter

[![Build Status](https://travis-ci.org/juttle/juttle-elastic-adapter.svg)](https://travis-ci.org/juttle/juttle-elastic-adapter)

The Juttle Elastic Adapter enables reading and writing documents using [Elasticsearch](https://www.elastic.co/products/elasticsearch). It works with Elasticsearch version 1.5.2 (including [AWS Elasticsearch Service](https://aws.amazon.com/elasticsearch-service/)) and above, such as version 2.1.1.

## Examples

Read all documents stored in Elasticsearch timestamped with the last hour:

```juttle
read elastic -from :1 hour ago: -to :now:
```

Write a document timestamped with the current time, with one field `{ name: "test" }`, which you'll then be able to query using `read elastic`.

```juttle
emit -limit 1 | put name="test" | write elastic
```

Read recent records from Elasticsearch that have field `name` with value `test`:

```juttle
read elastic -last :1 hour: name = 'test'
```

Read recent records from Elasticsearch that contain the text `hello world` in any field:

```juttle
read elastic -last :1 hour: 'hello world'
```

An end-to-end example is described [here](https://github.com/juttle/juttle-engine/blob/master/examples/elastic-newstracker/README.md) and deployed to the demo system [demo.juttle.io](http://demo.juttle.io/?path=/examples/elastic-newstracker/index.juttle). The [Juttle Tutorial](http://juttle.github.io/juttle/concepts/juttle_tutorial/) also covers using elastic adapter.

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
        "elastic": {
            "address": "localhost",
            "port": 9200
        }
    }
}
```

To connect to an Elasticsearch instance elsewhere, change the `address`
and `port` in this configuration.

This configuration can also support all config options for the `Client` exclude `hosts`. All of the available options/keys are following [here](https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html)

The value for `elastic` can also be an array of Elasticsearch host locations. Give each one a unique `id` field, and `read -id` and `write -id` will use the appropriate host.

The Juttle Elastic Adapter can also make requests to Amazon Elasticsearch Service instances, which requires a little more configuration. To connect to Amazon Elasticsearch Service, an entry in the adapter config must have `{"aws": true}` as well as `region`, `endpoint`, `access_key`, and `secret_key` fields. `access_key` and `secret_key` can also be specified by the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` respectively.

Here's an example Juttle Elastic Adapter configuration that can connect to a local Elasticsearch instance running on port 9200 using `read/write elastic -id "local"` and an Amazon Elasticsearch Service at `search-foo-bar.us-west-2.es.amazonaws.com` using `read/write elastic -id "amazon"`:

```json
{
    "adapters": {
        "elastic": [
            {
                "id": "local",
                "address": "localhost",
                "port": 9200
            },
            {
                "id": "amazon",
                "aws": true,
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

For schemas that create indices at regular intervals, the adapter supports an `indexInterval` option. Valid values for `indexInterval` are `day`, `week`, `month`, `year`, and `none`. With `day`, the adapter will use indices formatted `${index}${yyyy.mm.dd}`. With `week`, it will use `${index}${yyyy.ww}`, where `ww` ranges from 01 to 53 numbering the weeks in a year. With `month`, it will use `${index}${yyyy.mm}`, and with `year`, it will use `${index}${yyyy}`. With `none`, the default, it will use just one index entirely specified by `index`. When using `indexInterval`, `index` should be the non-date portion of each index followed by `*`.

Lastly, the adapter expects all documents in Elasticsearch to have a field containing a timestamp. By default, it expects this to be the `@timestamp` field. This is configurable with the `-timeField` option to `read` and `write`.

Specifics of using the default [Logstash](https://www.elastic.co/products/logstash) schema are described [here](./docs/logstash.md), including handling of analyzed vs not_analyzed string fields.

## Usage

### Read options

In addition to the options below, `read elastic` supports field comparisons of form `field = value`, that can be combined into filter expressions using `AND`/`OR`/`NOT` operators, and free text search, following the [Juttle filtering syntax](http://juttle.github.io/juttle/concepts/filtering/).

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
`optimize` | true/false | no | optional flag to disable optimized reads, see [Optimizations](#optimizations) | true

### Write options

Name | Type | Required | Description | Default
-----|------|----------|-------------|---------
`id` | string | no | write to the configured Elasticsearch endpoint with this ID | the first endpoint in `config.json`
`index` | string | no | index to write to | `juttle`
`indexInterval` | string | no | granularity of an index. valid options: `day` `week`, `month`, `year`, `none` | `none`
`type` | string | no | document type to write to | `event`
`timeField` | string | no | field containing timestamps | `@timestamp`
`idField` | string | no | if specified, the value of this field on each point will be used as the document ID for the corresponding Elasticsearch document and not stored | none
`chunkSize` | number | no | buffer points until `chunkSize` have been received or the program ends, then flush | 1024
`concurrency` | number | no | number of concurrent bulk requests to make to Elasticsearch (each inserts `<= chunkSize` points) | 10

### Optimizations

Whenever the elastic adapter can shape the entire Juttle flowgraph or its portion into an Elasticsearch query, it will do so, sending the execution to ES, so only the matching data will come back into Juttle runtime. The portion of the program expressed in `read elastic` is always executed as an ES query; the downstream Juttle processors may be optimized as well.

_Fully optimized example_

```juttle
read elastic -last :1 hour: -index 'scratch' -type 'tag1' name = 'test'
| reduce count()
```

This program will form an ES query that asks it do count the documents in `scratch` index with document type `tag1` whose field `name` is set to the value `test`, and only a single record (count) will come back from Elasticsearch.

_Less optimized example_

```juttle
read elastic -last :1 hour: name = 'test'
| put threshold = 42
| filter value > threshold
```

In this case, Juttle will issue a query against ES that matches documents whose field `name` is set to the value `test` (i.e. Juttle will not read *all* documents from ES, only the once that match the filter expression in `read elastic`). However, the rest of the program that filters for values exceeding threshold will be executing in the Juttle runtime, as it isn't possible to hand off this kind of filtering to ES.

#### List of optimized operations

* any filter expression or full text search as part of `read elastic` (note: `read elastic | filter ...` is not optimized)
* `head` or `tail`
* `reduce count()`, `sum()`, and other built-in reducers
* `reduce by fieldname` (other than reduce by document type)
* `reduce -every :interval:`

##### Optimization and nested objects
There are a few fundamental incompatibilities between [Elasticsearch's model](https://www.elastic.co/guide/en/elasticsearch/guide/current/complex-core-fields.html) for nested object and array fields and [Juttle's](http://juttle.github.io/juttle/concepts/fields/#fields-with-object-or-array-values). This can lead to some odd results for optimized programs. For objects, an optimized `reduce by some_object_field` will return `null` as the only value for `some_object_field`. For arrays, an optimized `reduce by some_array_field` will return a separate value for `some_array_field` for every element in every array stored in `some_array_field`. For results conforming to Juttle's `reduce` behavior, disable optimization with `read elastic -optimize false`.

In case of unexpected behavior with optimized reads, add `-optimize false` option to `read elastic` to disable optimizations, and kindly report the problem as a GitHub issue.

## Contributing

Want to contribute? Awesome! Don’t hesitate to file an issue or open a pull
request. See the common [contributing guidelines for project Juttle](https://github.com/juttle/juttle/blob/master/CONTRIBUTING.md).
