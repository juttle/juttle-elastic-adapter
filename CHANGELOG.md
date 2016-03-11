# Change Log

This file documents all notable changes to Juttle Elastic Adapter. The release
numbering uses [semantic versioning](http://semver.org).

## 0.6.0

Released 2016-03-10

### Minor Changes
- Updated to support Juttle 0.6.0.
- Added optimization support for stdev.
[#113](https://github.com/juttle/juttle-elastic-adapter/issues/113)
- Removed warnings when accessing nested points.
[#111](https://github.com/juttle/juttle-elastic-adapter/issues/111)

## 0.5.0

Released 2016-02-25

### Minor Changes

- Updated to be compatible with Juttle 0.5.0 [#104](https://github.com/juttle/juttle-elastic-adapter/issues/104)

## 0.4.2

Released 2016-02-19

### Bug Fixes

- Fixed read to require either a time range [#79](https://github.com/juttle/juttle-elastic-adapter/issues/79)
- Fixed write to create indices with non-analyzed string fields
[#89](https://github.com/juttle/juttle-elastic-adapter/issues/89)

## 0.4.1

Released 2016-02-02

### Bug Fixes

- Fixed conflict between configuration of aws and the -type option to read. [#67](https://github.com/juttle/juttle-elastic-adapter/issues/67)

## 0.4.0

Released 2016-02-01

### Minor Changes

- Added concurrency control for write. [#63](https://github.com/juttle/juttle-elastic-adapter/issues/63)

## 0.3.0

Released 2016-01-21

### Major Changes

- NOTICE: As part of the update to juttle 0.3.0, the configuration syntax for adapters changed from the name of the module (`"juttle-elastic-adapter"`) to the type of the adapter (`"elastic"`).
- Implement batched reduce optimization when there are many buckets using date histograms as opposed to a single query per batch. [#49]
- Added support to set the document type for read and write. [#44]
- Added support for nested objects in write. [#45]
- Update to support Juttle 0.3.0. [#56]

### Minor Changes

- Improved the error message for window overflow. [#54]
- Properly return an empty aggregation result when there is no matching data. [#38]

## 0.2.0

Released 2016-01-07

### Major Changes

- Added support to override the timeField for read and write. [#35]
- Added read support for configurable index intervals other than per day. [#30]

## 0.1.2

Released 2016-01-07

### Major Changes

- Added read support for a configurable index prefix. [#22]
- Updated to support Juttle 0.2.0 [#39], [#41]

## 0.1.0

Released 2016-01-06

- Initial Public Release
