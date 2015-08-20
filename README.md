# MySQL Pipelets for [Toubkal](https://github.com/ReactiveSets/toubkal)

[![NPM version](https://badge.fury.io/js/toubkal_mysql.png)](http://badge.fury.io/js/toubkal_mysql)

Stability: Experimental, needs CI testing.

## Usage

Getting a dataflow from "users" table in the "test" database using the "root" account:

```javascript
var rs = require( 'toubkal_mysql' );

rs.mysql( 'test.users', 'root' )
  .trace( 'users' )              // displays users' from NYC (per filter bellow)
  .filter( [ { city: 'NYC' } ] ) // i.e. SELECT users WHERE city = "NYC"
  .set()                         // Cache NYC users for downstream consumption
;
```

Where the "root" account is defined in configuration file "~/config.rs.json", hiding credentials, e.g.

```javascript
[
  {
    "id"      : "toubkal_mysql#root",
    "module"  : "toubkal_mysql",
    "name"    : "root",

    "mysql": {
      "host"    : "localhost",
      "user"    : "root",
      "password": "***"
    }
  }
]
```

Where "mysql" object provides options for connecting to MySQL database. [All connection options are from "mysql"
npm module](https://www.npmjs.com/package/mysql#connection-options).

## Install

```bash
$ npm install toubkal_mysql
```

## Documentation

### mysql( table, user, options )

Provides a Toubkal dataflow for MySQL "table", for user account "user".

Optional parameter options have optional attributes:
- configuration (String): filename, default is "~/config.rs.json"
- mysql (Object): [connection options for "mysql" npm module](https://www.npmjs.com/package/mysql#connection-options), main options are:
  - host (String): e.g. "localhost"
  - user (String): e.g. "root"
  - password (String): e.g. "therootpassword"
- key (Array of Strings): used for remove operations and build SQL DELETE, default is ['id'].
  On add and remove operations, all values must provide all key attributes.
- columns (String or Array of Strings): used for SELECT and INSERT default is '*'.
  On inserts, if colunns is not provided, it will be derived from added values which is somewhat slower.

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
