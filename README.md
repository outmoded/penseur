# penseur

Application-friendly RethinkDB client

[![Build Status](https://secure.travis-ci.org/hueniverse/penseur.png)](http://travis-ci.org/hueniverse/penseur)


## API

### `new Penseur.Db(name, [options])`

Creates a new `Db` object where:
- `name` - name of the database to use. Default is `'test'`.
- `options` - optional configuration:
  - `host` - string containing the host to connect to. Default is `'localhost'`
  - `port` - port to connect on. Default is `28015`.
  - `authKey` - string containing an established authorization key
  - `user` - string containing the user you want to perform actions as. Default is `'admin'`
  - `password` - string containing the users password
  - `timeout` - number indicating the number of seconds to wait for a connection to be established before timing out. Default is `20` seconds.
  - `ssl` - ssl settings when RethinkDB requires a secure connection. This setting is passed to the RethinkDB module.
    - `ca` - certificate as a Buffer used for establishing a secure connection to rethink, see [RethinkDB connect documentation](https://www.rethinkdb.com/api/javascript/connect/)
  - `test` - boolean indicating if the `Db` actions should take place. Useful for testing scenarios, do not use in production.
  - `extended` - object that is used to extend the members of the table objects.
  - `onConnect` - function to execute when a connection is established.
  - `onError` - function to execute when an error occurs. Signature is `function(err)` where `err` is an error object.
  - `onDisconnect`- function to execute when a disconnection occurs. Signature is `function(willReconnect)` where `willReconnect` is a boolean indicating if the connection will be reestablished.
  - `reconnect` - boolean indicating if the connection should be reestablished when interrupted
  - `reconnectTimeout` - number of milliseconds to wait between reconnect attempts, when `false` there is no timeout. Defaults to `100`


#### `db.connect(callback)`

Create a connection to the database. `callback` is a function with the signature `function(err)` that is executed when the connection is established or an error occurs establishing the connection.


#### `db.close([next])`

Close all database connections.


#### `db.establish([tables], callback)`

Note that this can alter data and indexes, not intended of production use.

Establish a connection if one doesn't exist and create the database and tables if they don't already exist. This function also decorates the `db` object with helper properties to for using each of the tables.

- `[tables]` - array of strings with the name of each table to create
- `callback` - function with signature `function(err)` that is executed when everything succeeds or an error is encountered.


#### `db.disable(table, method, [options])`

Only available when `Db` is constructed in test mode. Disable a specific method from being performed on a table, used for testing.

- `table` - name of table
- `method` - name of method on table to disable
- `options` - optional object with the following properties
  - `value` - value to return when the method is called. Can be an error object to simulate errors.


#### `db.enable(table, method)`

Only available when `Db` is constructed in test mode. Enable a disabled method on a table, used for testing.

- `table` - name of table
- `method` - name of method on table to enable


#### `db.r`

Property that contains the [RethinkDB module](https://www.npmjs.com/package/rethinkdb).


### Table functions

After a database connection exists and tables are established then the `Db` object is decorated with properties for each table name containing various helper function. Below are the available functions for each table.

#### `db[table].get(id, [options, ] callback)`

Retrieve a record in the `table` with the given `id`. `id` itself can be an array of `id` values if you want to retrieve multiple records.

- `id` - unique identifier of record to retrieve. Can be an array with values for each ID to retrieve.
- `options` - optional object with the following properties
  - `sort` - table key to sort results using
  - `order` - `'descending'` or `'ascending'` for sort order, Defaults to `'ascending'`
  - `from` - index in result set to select
  - `count` - number of records to return in results
  - `filter` - properties to pluck from the results
- `callback` - function with `function(err, results)` signature


#### `db[table].all(callback)`

Retrieve all records for a table.

- `callback` - function with `function(err, results)` signature


#### `db[table].exist(id, callback)`

Determine if a record in the `table` exists with the provided ID

- `id` - unique identifier of record to retrieve. Can be an array with values for each ID to retrieve.
- `callback` - function with `function(err, exists)` signature


#### `db[table].query(criteria, callback)`

Perform a query on the table using the provided criteria. Criteria is available on the `Db` object and is listed in the criteria section below.

- `criteria` - db [criteria](#criteria) functions chained together
- `callback` - function with `function(err, results)` signature


#### `db[table].single(criteria, callback)`

Retrieve a single record from the provided criteria.

- `criteria` - db [criteria](#criteria) functions chained together
- `callback` - function with `function(err, result)` signature


#### `db[table].count(criteria, callback)`

Retrieve the number of records in the table that match the given criteria.

- `criteria` - db [criteria](#criteria) functions chained together
- `callback` - function with `function(err, count)` signature


#### `db[table].insert(items, [options, ] callback)`

Create new record(s) in the table. Each item can specify a unique `id` property or allow rethink to generate one for them.

- `items` - item object or array of items to insert into the table
- `options` - optional object with the following properties
  - `merge` - boolean, when true any conflicts with existing items will result in an update, when false an error is returned.
- `callback` - function with `function(err, keys)` signature


#### `db[table].update(ids, changes, callback)`

Update an existing record with the provided changes.

- `ids` - an identifier or array of identifiers of records to update in the table
- `changes` - the parts of the record to change and the values to change the parts to
- `callback` - function with `function(err)` signature


#### `db[table].update(updates, callback)`

Update an existing record with the provided changes.

- `updates` - an array of records to update (each must include an existing primary key)
- `callback` - function with `function(err)` signature


#### `db[table].remove(criteria, callback)`

Remove the records in the table that match the given criteria.

- `criteria` - db [criteria](#criteria) functions chained together
- `callback` - function with `function(err)` signature


#### `db[table].empty(callback)`

Remove all records in the table.

- `callback` - function with `function(err)` signature


#### `db[table].sync(callback)`

Wait until all operations are complete and all data is persisted on permanent storage. Note that this function shouldn't be necessary for normal conditions.

- `callback` - function with `function(err)` signature


#### `db[table].index(indexes, callback)`

Create the secondary `indexes` on the table. `callback` is executed when all of the indexes are created.

- `indexes` - a string or array of strings for each index to create
- `callback` - function with `function(err)` signature


#### `db[table].changes(criteria, [options,] [callback])`

Subscribe to changes matching the given criteria for the table.

- `criteria` - db [criteria](#criteria) functions chained together
- `options` - optional object with the following properties
  - `handler` - handler function to execute when changes occur. When provided the `callback` function can be omitted.
  - `reconnect` - boolean, reconnect if the connection to the feed is interrupted
  - `initial` - boolean, include the initial results in the change feed
- `callback` - function with `function(err, changes)` signature


### Criteria

#### `Db.or(values)`
#### `Db.contains(values[, options])`
#### `Db.not(values)`
#### `Db.unset()`
#### `Db.increment(value)`
#### `Db.append(value[, options])`
#### `Db.override(value)`
#### `Db.is(operator, values, ...and)`
#### `Db.by(index, values)`
