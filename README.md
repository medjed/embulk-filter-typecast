# Typecast filter plugin for Embulk

[![Build Status](https://secure.travis-ci.org/sonots/embulk-filter-typecast.png?branch=master)](http://travis-ci.org/sonots/embulk-filter-typecast)

A filter plugin for Embulk to cast column type.

## Configuration

- **columns**: columns to retain (array of hash)
  - **name**: name of column (required)
  - **type**: embulk type to cast
  - **format**: specify the format of the timestamp (string, default is default_timestamp_format)
  - **timezone**: specify the timezone of the timestamp (string, default is default_timezone)
- **default_timestamp_format**: default timestamp format (string, default is `%Y-%m-%d %H:%M:%S.%N %z`)
- **default_timezone**: default timezone (string, default is `UTC`)
* **stop_on_invalid_record**: stop bulk load transaction if a invalid record is found (boolean, default is `false)

## Example

See [example.csv](./example/example.csv) and [example.yml](./example/example.yml).

## JSONPath

For `type: json` column, you can specify [JSONPath](http://goessner.net/articles/JsonPath/):

```yaml
{name: json_column, type: json, json_path: $.payload.key1}
```

Following operators of JSONPath are not supported:

* Multiple properties such as `['name','name']`
* Multiple array indexes such as `[1,2]`
* Array slice such as `[1:2]`
* Filter expression such as `[?(<expression>)]`

## Development

Run example:

```
$ ./gradlew gem
$ embulk preview -I build/gemContents/lib example/example.yml
```

Run test:

```
$ ./gradlew test
```

Run checkstyle:

```
$ ./gradlew check
```

Release gem:

```
$ ./gradlew gem
$ gem push build/gems/embulk-filter-typecast-<version>.gem
```
