# Logstash Support

The Juttle elastic adapter can work with an Elasticsearch instance that uses the [Logstash](https://www.elastic.co/products/logstash) schema. The specifics of talking to ES with the Logstash schema are described here.

## Schema

Logstash creates daily indices with names in the format `logstash-YYYY.MM.DD`, such as `logstash-2016.01.05`. By default, the adapter reads from all indices, so if you want to read from only Logstash's indices, use `logstash-*` for the `index` option.

If you have many days' worth of data, Logstash will create many indices. Reading from many indices can be slow in Elasticsearch. To speed things up in this case, `read elastic` can narrow its search to only the days it needs for its query. To get this behavior, use `-indexInterval "day"`. Also, `write elastic` requires `-indexInterval "day"` for writing into daily indices.

Another important issue in using the Logstash schema is whether fields are analyzed.

## Querying not_analyzed fields

Elasticsearch makes a distinction between string fields that are "analyzed" (that is, split into tokens for searchability) and "not_analyzed". This is specified in the [mapping](https://www.elastic.co/guide/en/elasticsearch/guide/current/mapping-intro.html).

Using the [Logstash index template](https://github.com/logstash-plugins/logstash-output-elasticsearch/blob/b85e72e8c160100a84a6ebc341fda88f58de0999/lib/logstash/outputs/elasticsearch/elasticsearch-template.json), Logstash will populate Elasticsearch with string fields that are analyzed. In addition, for all string fields except `message`, it will create a not_analyzed subfield with name `<fieldname>.raw`.

A simple Juttle `read` of such fields will return the string value for each `fieldname` as one would expect. However, aggregations such as `reduce by <fieldname>` will be interpreted by ES as terms queries on analyzed versions of the fields. The result of such a query may be surprising the first time you run into it. See Elastic/Logstash's own [example](https://www.elastic.co/blog/logstash-1-3-1-released).

Let's say you put 2 entries with the field `path` into ES via Logstash, one with value `/docs/processors/reduce.md` and the other with value `/docs/index.md`. Then you issue a Juttle query:

```
read elastic ...
| reduce by path;

┌──────────────────┐
│ path             │
├──────────────────┤
│ docs             │
├──────────────────┤
│ processors       │
├──────────────────┤
│ reduce.md        │
├──────────────────┤
│ index.md         │
└──────────────────┘

```

The result of this are the 4 individual tokens (terms) instead of the 2 full-path values you may have expected. To look at the raw, not_analyzed strings instead of the terms, the Juttle query should be:

```
read elastic ...
| reduce by 'path.raw';

┌─────────────────────────────────────────┐
│ path.raw                                │
├─────────────────────────────────────────┤
│ /docs/processors/reduce.md              │
├─────────────────────────────────────────┤
│ /docs/index.md                          │
└─────────────────────────────────────────┘

```

Notice that the field name in the resulting data will be `path.raw` and not `path` (Juttle preserves the field name you used in the `reduce` directive). You can always rename the field:

```
read elastic ...
| reduce by 'path.raw'
| put path = *'path.raw'
| remove 'path.raw';

┌─────────────────────────────────────────┐
│ path                                    │
├─────────────────────────────────────────┤
│ /docs/processors/reduce.md              │
├─────────────────────────────────────────┤
│ /docs/index.md                          │
└─────────────────────────────────────────┘
```

The above applies to optimized execution where the elastic adapter hands off execution (including the `reduce` step) to Elasticsearch. If the program is running without optimization, the field references will be by regular field names without `.raw` suffix.

The `message` field only has an analyzed string in the Logstash mapping, and does not have a `message.raw`, the assumption being that full log message strings have near-unique values that would only be searched on, and not filtered on by exact match of the entire string. This means the following Juttle programs would not work against a default Logstash schema:

```
read elastic ... message = 'This exact message';

read elastic ...
| reduce by 'message.raw';
```

And this Juttle program would produce a larger set of unique single-word tokens, rather than a smaller set of full log messages:

```
read elastic ...
| reduce by message
```

If these semantics are not desired, choose a different ES mapping instead of the default Logstash one.
