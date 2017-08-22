## DBWrkr PostgreSQL storage engine

[![Build Status](https://travis-ci.org/HPieters/dbwrkr-postgresql.svg?branch=master)](https://travis-ci.org/HPieters/dbwrkr-postgresql.svg?branch=master)

PostgreSQL storage engine for [DBWrkr](https://github.com/whyhankee/dbwrkr). 

## Links

DBWrkr pub-sub system <https://github.com/whyhankee/dbwrkr>

## Requirements

- Requires PostgreSQL 9.5+ (Support for Upsert and jsonb)

## Changelog

v0.1.1
* Rewrote queries to use placeholders for sanitization 

v0.1.0
* Rewrote from Massive to pg module, 
* Create database if does nog exist
* Pass 100% of tests 

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