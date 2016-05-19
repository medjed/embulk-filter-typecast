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

## JSONPath (like) name

For `type: json` column, you can specify [JSONPath](http://goessner.net/articles/JsonPath/) for column's name as:

```
$.payload.key1
$.payload.array[0]
$.payload.array[*]
```

NOTE: JSONPath syntax is not fully supported

## ToDo

* Write test

## Development

Run example:

```
$ ./gradlew classpath
$ embulk preview -I lib example/example.yml
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
$ ./gradlew gemPush
```
