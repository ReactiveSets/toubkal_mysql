# MySQL Pipelets for [Toubkal](https://github.com/ReactiveSets/toubkal)

Not yet published to npm

[![NPM version](https://badge.fury.io/js/toubkal_mysql.png)](http://badge.fury.io/js/toubkal_mysql)

API Stability: Unstable, this API is under development

Needs Travis CI testing

## Install

```bash
$ npm install toubkal_mysql
```

## Features:
- Toubkal pipelet: get updates in real-time
- Hides MySQL credentials in JSON configuration file
- Waits indefinitely for ready MySQL connection to process operations
- Stateless, does not keep anything in memory, relies on MySQL cache
- Can be cached explitity using set() pipelet
- Dynamically creates optimized MySQL queries:
  - SELECT queries from downsteam pipelet queries provided by Toubkal filter() pipelets
  - DELETE queries from upstream remove operations using "key" option
  - INSERT queries from upstream add opeations using "columns" parameter
- Allows column name aliases
- Emits detailled errors in error dataflow for downstream error reporting and recovery by reverting
failed operations

## Usage

### Reading 'test.users' table

Getting a dataflow from "users" table in the "test" database using the "root" MySQL account:

```javascript
var rs = require( 'toubkal_mysql' );

rs.mysql( 'test.users',
    , [ 'id', 'email', 'first_name', 'last_name', 'city' ]
  )
  
  .filter( [ { city: 'NYC' } ] ) // Query using SELECT * FROM users WHERE city = "NYC"

  .trace( 'users' )              // displays users' from NYC

  .set()                         // Cache NYC users for downstream consumption
;
```

### Updating 'test.users' Table

Updating users table only requires to connect a dataflow upstream of mysql - e.g. connecting authorized clients'
upstream of mysql():
```javascript
var rs = require( 'toubkal_mysql' );

authorized_clients
    
  // Upstream (of mysql): updates, for DELETE and INSERT
  
  .mysql( 'test.users',
    [ 'id', 'email', 'first_name', 'last_name', 'city' ]
  )
  
  // Downstream (of mysql): fetching, and real-time updates
  
  ._add_destination( authorized_clients )
;
```

Where "authorized_clients" is a dataflow of authorized updates from clients;


### Account Credentials Hiding in Configuration

The "root" account is defined in configuration file "~/config.rs.json", hiding credentials, e.g.

```javascript
[
  {
    "id"      : "toubkal_mysql#root",
    "module"  : "toubkal_mysql",
    "name"    : "root",

    "mysql": {
      "host"    : "localhost",
      "user"    : "root",
      "password": "<password for root account>"
    }
  }
]
```

Where "mysql" object provides options for connecting to MySQL database. [All connection options are from "mysql"
npm module](https://www.npmjs.com/package/mysql#connection-options).

Many MySQL servers configuration can be defined in the same JSON configuration
file by adding objects with a unique "id" assigned with the value "toubkal_mysql#<account name>".

## Reference

### mysql( table, columns, options )

Provides a Toubkal dataflow for MySQL "table".

Parameters:
- table (String): MySQL table name. The table must exist in MySQL and must have a primary key
  that will be identical to the Pipelet's key unless aliased (see columns definition bellow).
- columns (Array): defines all desired columns, and MUST include the primary key, to create
  SELECT, DELETE and INSERT queries. Each column is defined as:
  - (String): column name
  - (Object):
    - id: MySQL column name
    - as: dataflow attribute name, default is id
- options (Object): optional attributes:
  - connection (String): MySQL connection identifier in configuration file, default is "root"
  - configuration (String): filename of configuration file, default is "~/config.rs.json"
  - mysql (Object): 
    [connection options for "mysql" npm module](https://www.npmjs.com/package/mysql#connection-options).
    These options supercede those from the configuration file, main options are:
    - host (String): e.g. "localhost"
    - user (String): e.g. "root"
    - password (String): e.g. "therootpassword"
    - database (String): e.g. "test"
  - key (Array of Strings): defines the primary key, may be aliased with columns parameter
    definition. default is [ 'id' ]

## Licence

  The MIT License (MIT)

  Copyright (c) 2015, Reactive Sets

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.
