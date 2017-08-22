## DBWrkr PostgreSQL storage engine

**Warning**, work in progres not yet thoroughly tested to be published.

PostgreSQL storage engine for [DBWrkr](https://github.com/whyhankee/dbwrkr). 

## Links

DBWrkr pub-sub system <https://github.com/whyhankee/dbwrkr>

## Requirements

- Requires PostgreSQL 9.3+ (Support for Upsert)

## Changelog

v0.0.5
* Rewrote fetchNext to raw query to get desired functionality 

v0.0.4
* Add pool disconnect to disconnect function 

v0.0.3
* Bugfix always return string from fieldMapper function for `tid` 

v0.0.2
* Bugfix when connecting without tables caused crash

v0.0.1
* Initial release