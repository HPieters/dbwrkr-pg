## DBWrkr PostgreSQL storage engine

[![Build Status](https://travis-ci.org/HPieters/dbwrkr-pg.svg?branch=master)](https://travis-ci.org/HPieters/dbwrkr-pg.svg?branch=master)

PostgreSQL storage engine for [DBWrkr](https://github.com/whyhankee/dbwrkr). 

## Links

DBWrkr pub-sub system <https://github.com/whyhankee/dbwrkr>

## Requirements

- Requires PostgreSQL 9.5+ (Support for Upsert and jsonb)

## Changelog

v0.1.6
* Fixed bug in setup, misspelled port caused setup to crash on ports other than default. Also added catch handler to handle correctly.

v0.1.5
* Corrected double indexes

v0.1.4
* Add cache library, cleanup, move functions to utils

v0.1.3
* Change architecture to a relational schema

v0.1.2
* Rewrote to support Node.js v4.*

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
