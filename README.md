# penseur

Application-friendly RethinkDB client

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


#### `await db.connect()`

Create a connection to the database. Throws connection errors.


#### `await db.close()`

Close all database connections.


#### `await db.establish([tables])`

Note that this can alter data and indexes, not intended of production use.

Establish a connection if one doesn't exist and create the database and tables if they don't already exist. This function also decorates the `db` object with helper properties to for using each of the tables.

- `[tables]` - array of strings with the name of each table to create


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

#### `await db[table].get(id, [options])`

Retrieve a record in the `table` with the given `id`. `id` itself can be an array of `id` values if you want to retrieve multiple records.

- `id` - unique identifier of record to retrieve. Can be an array with values for each ID to retrieve.
- `options` - optional object with the following properties
  - `sort` - table key to sort results using
  - `order` - `'descending'` or `'ascending'` for sort order, Defaults to `'ascending'`
  - `from` - index in result set to select
  - `count` - number of records to return in results
  - `filter` - properties to pluck from the results


#### `await db[table].all()`

Retrieve all records for a table.


#### `await db[table].exist(id)`

Determine if a record in the `table` exists with the provided ID

- `id` - unique identifier of record to retrieve. Can be an array with values for each ID to retrieve.


#### `await db[table].query(criteria)`

Perform a query on the table using the provided criteria. Criteria is available on the `Db` object and is listed in the criteria section below.

- `criteria` - db [criteria](#criteria) functions chained together


#### `await db[table].single(criteria)`

Retrieve a single record from the provided criteria.

- `criteria` - db [criteria](#criteria) functions chained together


#### `await db[table].count(criteria)`

Retrieve the number of records in the table that match the given criteria.

- `criteria` - db [criteria](#criteria) functions chained together


#### `await db[table].insert(items, [options])`

Create new record(s) in the table. Each item can specify a unique `id` property or allow rethink to generate one for them.

- `items` - item object or array of items to insert into the table
- `options` - optional object with the following properties
    - `merge` - boolean, when true any conflicts with existing items will result in an update, when false an error is returned
	- `chunks` - maximum number of updates to send to the database at the same time


#### `await db[table].update(ids, changes)`

Update an existing record with the provided changes.

- `ids` - an identifier or array of identifiers of records to update in the table
- `changes` - the parts of the record to change and the values to change the parts to


#### `await db[table].update(updates, [options])`

Update an existing record with the provided changes.

- `updates` - an array of records to update (each must include an existing primary key)
- `options` - optional settings where:
	- `chunks` - maximum number of updates to send to the database at the same time


#### `await db[table].remove(criteria)`

Remove the records in the table that match the given criteria.

- `criteria` - db [criteria](#criteria) functions chained together


#### `await db[table].empty()`

Remove all records in the table.


#### `await db[table].sync()`

Wait until all operations are complete and all data is persisted on permanent storage. Note that this function shouldn't be necessary for normal conditions.


#### `await db[table].index(indexes)`

Create the secondary `indexes` on the table.

- `indexes` - a string or array of strings for each index to create


#### `await db[table].changes(criteria, [options])`

Subscribe to changes matching the given criteria for the table.

- `criteria` - db [criteria](#criteria) functions chained together
- `options` - optional object with the following properties
  - `handler` - handler function to execute when changes occur.
  - `reconnect` - boolean, reconnect if the connection to the feed is interrupted
  - `initial` - boolean, include the initial results in the change feed


### Criteria

#### `Db.or(values)`
#### `Db.contains(values[, options])`
#### `Db.not(values)`
#### `Db.unset()`
#### `Db.empty()`
#### `Db.increment(value)`
#### `Db.append(value[, options])`
#### `Db.override(value)`
#### `Db.is(operator, values, ...and)`
#### `Db.by(index, values)`
