## DBWrkr PostgreSQL storage engine

**Warning**, work in progres not yet thoroughly tested to be published.

PostgreSQL storage engine for [DBWrkr](https://github.com/whyhankee/dbwrkr). 

## Links

DBWrkr pub-sub system <https://github.com/whyhankee/dbwrkr>

## Requirements

- Requires PostgreSQL 9.3+ (Support for Upsert)
- Requires the database to be created in PostgreSQL

## Todo & considerations

- [ ] Check if Database exists, if not create.
- [ ] Correct implementation of criteria from *find* and *remove*
- [ ] Maybe promise based rather than callbacks (dbwrkr)
- [ ] Performance issues of using an array of strings rather than relational schema
- [ ] Test Parent feature of an item
- [ ] Test mapper is just there to pass the test...
- [ ] Map id to string, why?
- [ ] tid an Integer?

## Changelog

v0.0.1
* Initial release