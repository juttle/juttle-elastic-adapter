# Juttle Elastic Adapter

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

## Usage

### Read options


Name | Type | Required | Description
-----|------|----------|-------------
`from` | moment | no | select points after this time (inclusive)
`to`   | moment | no | select points before this time (exclusive)
`last` | duration | no | select points within this time in the past (exclusive)
`id` | string | no | Read from the configured Elasticsearch endpoint with this ID

### Write options

Name | Type | Required | Description
-----|------|----------|-------------
`id` | string | no | Write to the configured Elasticsearch endpoint with this ID

## Contributing

Want to contribute? Awesome! Don’t hesitate to file an issue or open a pull
request.
