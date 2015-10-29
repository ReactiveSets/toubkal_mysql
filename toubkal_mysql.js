/*  toubkal_mysql.js

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
*/
'use strict';

var mysql            = require( 'mysql' )
  , rs               = require( 'toubkal' )
  , RS               = rs.RS
  , uuid             = RS.uuid
  , timestamp_string = RS.timestamp_string
  , Pipelet          = RS.Pipelet
  , Greedy           = RS.Greedy
  , Set              = RS.Set
  , Query            = RS.Query
  , extend           = RS.extend
  , clone            = extend.clone
  , log              = RS.log.bind( null, 'mysql' )
  , de               = true
  , ug               = de && log
  , slice            = Array.prototype.slice
;

module.exports = rs; // re-exports rs which is the namespace for toubkal_mysql pipelets

/* ------------------------------------------------------------------------------------------------
   mysql_connections( options )
   
   Parameters:
   - options (optional Object): optional attributes:
     - mysql (Object): mysql npm module default connection options:
       https://www.npmjs.com/package/mysql#connection-options
*/

function MySQL_Connections( options ) {
  Set.call( this, [], options );
} // MySQL_Connections()

Set.Build( 'mysql_connections', MySQL_Connections, function( Super ) {
  return {
    _add_value: function( t, connection ) {
      var that = this;
      
      connection = clone( connection );
      
      // Transactions are connection-based, shared by all pipelets sharing the same connection
      connection.transactions = {};
      
      connection.mysql = extend( {}, this._options.mysql, connection.mysql );
      
      var mysql_connection = mysql.createConnection( connection.mysql );
      
      // hide paswword, preventing downstream traces from disclosing it
      connection.mysql.password = connection.mysql.password && '***';
      
      de&&ug( this._get_name( '_add_value' ) + 'mysql:', connection.mysql );
      
      // Try to connect immediately
      mysql_connection.connect( function( error ) {
        if ( error ) {
          log( 'Error connecting to:', connection.mysql, ', error:', error );
          
          connection.mysql_connection = null;
          connection.error = error;
        } else {
          connection.mysql_connection = mysql_connection;
          mysql_connection.toJSON = function() { return 'mysql connection' };
        }
        
        Super._add_value.call( that, t, connection );
      } );
      
      // ToDo: monitor disconnections, automatically reconnect, possibly after some timeout
      
      return this;
    }, // _add_value()
    
    _remove_value: function( t, connection ) {
      var i = this._index_of( connection );
      
      if ( i != -1 ) {
        connection = this.a[ i ];
        
        // ToDo: handle transactions, either by terminating them now or waiting some time for them to terminate
        
        connection.mysql_connection && connection.mysql_connection.destroy();
        
        Super._remove_value.call( this, t, connection );
      } else {
        log( 'Error removing not found connection:', connection );
      }
      
      return this;
    } // _remove_value()
  };
} ); // mysql_connections()

/* ------------------------------------------------------------------------------------------------
   Converters
*/
var converters = ( function() {
  var converters = {};
  
  return {
    get: function( converter ) {
      return typeof converter == 'string' ? converters[ converter ] : converter;
    },
    
    set: function( name, converter ) {
      converters[ name ] = converter;
    }
  };
} )(); // converters

converters.set( 'uuid_b16', {
  parse: function( id ) {
    return uuid.parse( id, new Buffer( 16 ) );
  },
  
  serialize: function( id ) {
    return uuid.unparse( id );
  }
} ); // uuid_b16

converters.set( 'timestamp_t3', {
  parse: function( t ) {
    return t;
  },
  
  serialize: function( t ) {
    return timestamp_string( new Date( t ) );
  }
} ); // 'timestamp_t3'

/* ------------------------------------------------------------------------------------------------
   mysql_read( table, columns, connection, options )
   
   Parameters:
   - table (String): mysql table name
   - columns (Array of Columns): see mysql() for full definition of Column
   - connection (Pipelet): mysql_connections() output (will use the last added)
   - options (optional Object): optional attributes:
     - key (Array of Strings):  field names used to build WHERE clause for DELETE, may be aliased
       by columns
   
   ToDo: implement trigger pipelet to pipe changes into process, generating a dataflow of changes to be read
*/
function MySQL_Read( table, columns, connection, options ) {
  var that = this;
  
  this._output || ( this._output = new MySQL_Read.Output( this, 'mysql_read_out', columns ) );
  
  this._table = table;
  this._mysql_connection = null;
  
  connection
    .greedy()
    ._output
    .on( "remove", remove_connection )
    .on( "add", add_connection )
  ;
  
  Greedy.call( this, options );
  
  function add_connection( connections ) {
    if ( connections.length ) {
      that._mysql_connection = connections[ connections.length - 1 ].mysql_connection;
    }
    
    that._output.call_receivers();
  } // add_connection()
  
  function remove_connection() {
    that._mysql_connection = null;
  } // remove_connection()
} // MySQL_Read()

MySQL_Read.Output = Greedy.Output.subclass(
  'MySQL_Read.Output',
  
  function( p, name, columns ) {
    this.receivers = [];
    this.columns = columns;
    
    Greedy.Output.call( this, p, name )
  },
  
  {
    call_receivers: function() {
      var p = this.pipelet
        , receivers = this.receivers
        , fetch = this._fetch
      ;
      
      while ( p._mysql_connection && receivers.length ) {
        fetch.apply( this, receivers.shift() );
      }
      
      return this;
    }, // call_receivers()
    
    add_receiver: function( _arguments ) {
      this.receivers.push( slice.call( _arguments ) );
      
      return this;
    }, // add_receiver()
    
    /* --------------------------------------------------------------------------------------------
       _fetch( receiver, query )
       
       SELECT values from table, according to query
    */
    _fetch: function( receiver, query ) {
      var p = this.pipelet
        , mysql_connection = p._mysql_connection
      ;
      
      if ( ! mysql_connection ) return this.add_receiver( arguments );
      
      var table = mysql_connection.escapeId( p._table )
        , columns = this.columns
        , where = ''
        , that = this
        , columns_aliases = []
        , parsers = {}
        , serializers = []
      ;
      
      // Get columns string and fill-up columns_aliases[]
      columns = columns.map( column_to_sql ).join( '\n       , ' );
      
      if ( query ) where = where_from_query( query, mysql_connection, columns_aliases, parsers );
      
      var sql = '  SELECT ' + columns + '\n\n  FROM ' + table + where;
      
      de&&ug( name() + ', sql:\n\n' + sql + '\n' );
      
      mysql_connection.query( sql, function( error, results, fields ) {
        if ( error ) {
          log( name() + ', unable to read', table, ', error:', error );
          
          // ToDo: handle errors
          
          return;
        }
        
        var s = serializers.length
          , serialize
          , column
          , i
          , result
        ;
        
        // Loop through serializers first because the number of columns with serializers
        // is expected to be much smaller on average than the number rows returned by SELECT.
        // This allows to maximize the amount of time spent serializing columns in the inner
        // loop.
        // Also, the serializers loop has more code than the row loop which would be slower
        // if it was the inner loop.
        while ( s ) {
          serialize = serializers[ --s ];
          
          column    = serialize.id;
          serialize = serialize.serialize;
          
          de&&ug( name() + ', serialize column:', column, 'with:', serialize );
          
          i = results.length;
          
          while ( i ) {
            result = results[ --i ];
            
            result[ column ] = serialize( result[ column ] );
          } // while there are results
        } // while there are serializers
        
        if ( query ) {
          de&&ug( name() + ', results before query filter:', results.length );
          
          results = new Query( query ).generate().filter( results );
        }
        
        de&&ug( name() + ', results:', results.length );
        
        receiver( results, true );
      } )
      
      function column_to_sql( column ) {
        var a = column
          , id
          , as
          , converter
        ;
        
        if ( typeof column === 'object' ) {
          id = column.id;
          as = column.as;
          converter = column.converter;
          
          column = id;
          a = as || id;
          
          if ( converter ) {
            converter = converters.get( converter );
            
            parsers[ a ] = converter.parse;
            serializers.push( { id: a, serialize: converter.serialize } );
          }
        }
        
        columns_aliases[ a ] = column;
        
        column = mysql_connection.escapeId( column );
        
        if ( as ) {
          column += ' AS ' + mysql_connection.escapeId( as );
        }
        
        return column;
      } // column_to_sql()
      
      function name() {
        return that._get_name( '_fetch' );
      } // name()
    } // _fetch()
  } // MySQL_Read.Output instance methods
); // MySQL_Read.Output

function where_from_query( query, connection, columns_aliases, parsers ) {
  var where = query
    .map( function( or_term ) {
      de&&ug( 'where_from_query(), or_term:', or_term );
      
      or_term = Object.keys( or_term )
        
        .map( function( property ) {
          var value = or_term[ property ]
            , parser
          ;
          
          if ( property === 'flow' && value === 'error' ) {
            return false;
          }
          
          switch ( toString.call( value ) ) {
            case '[object Number]':
            case '[object String]':
              // scalar values where strict equality is desired
              
              if ( parser = parsers[ property ] ) value = parser( value );
              
              var alias = columns_aliases[ property ];
              
              if ( ! alias ) throw new Error( 'where_from_query() error, column from query not defined in schema: ' + property );
              
              // ToDo: use "property" COLLATE latin1_bin = value, or utf8_bin for case-sensitive comparison
            return connection.escapeId( alias ) + ' = ' + connection.escape( value );
            
            case '[object Array]': // expression
            return translate_expression( connection, property, value );
            
            default:
            return false;
          }
        } )
        
        .filter( not_empty )
        
        .join( ' AND ' )
      ;
      
      return or_term || false;
    } )
    
    .filter( not_empty )
    
    .join( ' )\n     OR ( ' )
  ;
  
  return where ? '\n\n  WHERE ( ' + where + ' )' : '';
  
  function not_empty( v ) { return !!v }
  
  function translate_expression( connection, property, expression ) {
    /*
      This is work in progress, don't translate to SQL for now.
      
      The containing expression will therefore return more results than needed.
      
      Results will then be futher filtered by with the compiled query.
    */
    return false;
    
    var i = 0, sql = '';
    
    while ( i < expression.length ) {
      var first = expression[ i++ ], type = typeof first;
      
      switch( type ) {
        default:
          // unknown or unsupported operator type
        return false;
        
        case 'object': // this is a subexpression
          // There is no equivalent in SQL
          // need to abort expression generation
        return false;
        
        case 'string': // this is an operator
          switch( first ) {
            case 'failed':
              // this is like the not operator but applying to previous result
              sql = 'NOT ( ' + sql + ' )';
            break;
            
            case '||':
              sql = '( ' + sql + ') OR ';
            break;
            
            case '==':
              first = '=';
            // fall-through
            case '!=':
            case '>' :
            case '>=':
            case '<' :
            case '<=':
              sql += '( ' + property + ' ' + first + ' '
            break;
            
            default:
              // unsuported operator
            return false;
          } // switch( first )
        break;
      } // switch( type )
    } // while there are terms in expression
  } // translate_expression()
} // where_from_query()

Greedy.Build( 'mysql_read', MySQL_Read );

/* ------------------------------------------------------------------------------------------------
   mysql_write( table, columns, connection, options )
   
   Parameters:
   - table (String): mysql table name
   - columns (Array of Columns): see mysql() for full definition of Column
   - connection (Pipelet): mysql_connections() output (will use the last added)
   - options (optional Object): optional attributes:
     - key (Array of Strings): the set of fileds that uniquely define objects and used to build
       a WHERE clause for DELETE queries. May be aliased by columns
*/
function MySQL_Write( table, columns, connection, options ) {
  this._table            = table;
  this._columns          = columns;
  this._mysql_connection = null;
  this._waiters          = [];
  
  var column_ids      = this._column_ids      = []
    , aliases         = this._aliases         = []
    , columns_aliases = this._columns_aliases = {}
    , parsers         = this._parsers         = {}
    , that = this
  ;
  
  columns.forEach( add_column );
  
  connection
    .greedy()
    ._output
    .on( "add", add_connection )
    .on( "remove", remove_connection )
  ;
  
  Greedy.call( this, options );
  
  // return this; // if called with new
  // return undefined; // if called without new
  
  function add_column( column ) {
    var as = column, id, converter;
    
    if ( typeof column === 'object' ) {
      id = column.id;
      as = column.as || id;
      converter = column.converter;
      column = id;
      
      if ( converter ) parsers[ as ] = converters.get( converter ).parse;
    }
    
    column_ids.push( column );
    aliases   .push( as     );
    
    columns_aliases[ as ] = column;
  } // add_column()
  
  function add_connection( connections ) {
    var l = connections.length;
    
    if ( l ) {
      that._mysql_connection = connections[ l - 1 ].mysql_connection;
      
      that._call_waiters();
    }
  } // add_connection()
  
  function remove_connection() {
    that._connection = null;
  } // remove_connection()
} // MySQL_Write()

function null_key_attribute_error( position, attribute, value ) {
  return {
    code: 'NULL_KEY_ATTRIBUTE',
    position: position,
    attribute: attribute,
    message: 'Key attribute "' + attribute + '" value must be defined',
    error_value: value
  };
} // null_key_attribute_error()

Greedy.Build( 'mysql_write', MySQL_Write, function( Super ) { return {
  /* ----------------------------------------------------------------------------------------------
     _add_waiter( method, parameters )
     
     Add a MySQL connection waiter for method with parameters
     
     Parameters:
     - method (String): this instance method name e.g. "_add" or "_remove"
     - parameters (Array): parameters to call method when MySQL connection is ready
  */
  _add_waiter: function( method, parameters ) {
    var that = this;
    
    de&&ug( this._get_name( '_add_waiter' ) + 'method:' + method );
    
    this._waiters.push( { method: method, parameters: parameters } );
    
    return this;
  }, // _add_waiter()
  
  /* ----------------------------------------------------------------------------------------------
     _call_waiters()
     
     Call MySQL connection waiters as long as MySQL connection is ready
  */
  _call_waiters: function() {
    var name = de && this._get_name( '_call_waiter' ) + 'calling method:'
      , waiter
    ;
    
    while( this._mysql_connection && ( waiter = this._waiters.shift() ) ) {
      de&&ug( name, waiter.method );
      
      this[ waiter.method ].apply( this, waiter.parameters );
    }
    
    return this;
  }, // _call_waiters()
  
  /* ----------------------------------------------------------------------------------------------
     _add( values, options )
  */
  _add: function( values, options ) {
    var that = this
      , name = de && this._get_name( '_add' )
      , emit_values = []
    ;
    
    if ( values.length === 0 ) return emit(); // nothing
    
    var connection = this._mysql_connection;
    
    if ( ! connection ) return this._add_waiter( '_add', arguments );
    
    var column_ids = this._column_ids;
    
    if ( column_ids.length === 0 ) return emit(); // nothing
    
    // ToDo: handle transactions in options._t.id
    
    var bulk_values = make_bulk_insert_list( values, emit_values );
    
    if ( typeof bulk_values !== 'string' ) { // this is an error object
      // ToDo: send error to error dataflow
      emit_error( bulk_values );
      
      return this;
    }
    
    var table = connection.escapeId( this._table )
      , columns = '\n\n    ( ' + column_ids.map( escape_id ).join( ', ' ) + ' )'
      , sql = 'INSERT ' + table + columns + bulk_values
    ;
    
    de&&ug( name + 'sql:\n\n  ' + sql + '\n' );
    
    // All added values should have been removed first, the order of operations is important for MySQL
    connection.query( sql, function( error, results ) {
      if ( error ) {
        log( 'Unable to INSERT INTO', table
          , ', code:'    , error.code
          , ', number:'  , error.errno
          , ', sqlState:', error.sqlState
          , ', index:'   , error.index
          , ', message:' , error.message
          //, ', error:'   , error
        );
        
        /*
          ToDo: Error Handling:
          - Duplicate key: this should not happen since removes should be done first however:
            - these could be stored in an anti-state if unordered removes are desired
            - this may happen if someone added the conflicting value in the background,
              then consider updating
            - Example:
              { [Error: ER_DUP_ENTRY: Duplicate entry '100000' for key 'PRIMARY'] code: 'ER_DUP_ENTRY', errno: 1062, sqlState: '23000', index: 0 }
              
          - Connection error:
            - should atempt to reconnect, if it fails continuously, then the application may not
              be able to function, consider terminating the process
              
          - Constraints violations
          
            Emit errors with sender information so that added values may be removed by sender.
            Emitting to sender requires sender identification to be present either in incomming
            options or inband. Incomming options are a good candidate for sender identification
            inserted by clients' server.
            
            Because filter queries are used for authorizations and routing and that these do not
            interpret options, sender information must be emitted inband, allowing senders to
            query errors for updates they emitted.
            
            Emitted errors should be persistance-implementation-agnostic to allow senders to
            interpret errors regardless of low-level presistance implementations. This means
            that MySQL error code cannot be provided into error.
            
            These errors should be translatable into removes to allow the final state of senders'
            stateful pipelets to be updated.
            
            Finaly a mechanism must exist to stop the propagation of errors once state has been
            updated to prevent removes() to be propagated back to server, potentially alterring
            the valid state of the database.
        */
        emit_error( {
          // ToDo: provide toubkal error code from MySQL error
          
          engine: 'mysql',
          
          mysql: {
            table   : that._table,
            code    : error.code,
            number  : error.errno,
            sqlState: error.sqlState,
            index   : error.index,
            message : error.message,
            sql     : sql
          }
        } )
        
        return;
      }
      
      // ToDo: if results.affectedRows != values.length, we have a problem
      de&&ug( name + 'inserted rows:', results.affectedRows );
      
      emit(); // valid values
    } )
    
    return this;
    
    /* --------------------------------------------------------------------------------------------
       make_bulk_insert_list( values, emit_values )
       
       Make bulk insert list and make emit values, limited to actual columns.
       
       That way a read on the table should return the same values as emited values
       
       Missing attributes will be set as null unless part of the key in which case an error is
       returned.
       
       There still may be some discrepencies if columns is not specified and some values have
       undefined columns.
       
       Returns:
         String: bulk_values
         Object: error
    */
    function make_bulk_insert_list( values, emit_values ) {
      var key     = that._options.key
        , columns = that._aliases
        , parsers = that._parsers
        , bulk_values = '\n\n  VALUES\n'
        , vl = values.length
        , cl = columns.length
      ;
      
      for ( var i = -1; ++i < vl; ) {
        var value = values[ i ]
          , emit_value = emit_values[ i ] = {}
          , c, v, parser
        ;
        
        bulk_values += ( i ? ',\n    ' : '\n    ' );
        
        for ( var j = -1; ++j < cl; ) {
          c = columns[ j ];
          v = value[ c ];
          
          if ( v === null || v === undefined ) {
            if ( key.indexOf( c ) !== -1 ) {
              // this attribute is part of the key, it must be provided
              return null_key_attribute_error( i, c, value );
            }
            
            v = null;
          }
          
          emit_value[ c ] = v;
          
          if ( parser = parsers[ c ] ) v = parser( v );
          
          bulk_values += ( j ? ', ' : '( ' ) + connection.escape( v );
        }
        
        bulk_values += ' )';
      }
      
      return bulk_values;
    } // make_bulk_insert_list()
    
    function escape_id( id ) {
      return connection.escapeId( id );
    }
    
    function emit() {
      return that.__emit_add( emit_values, options );
    } // emit()
    
    function emit_error( error ) {
      error.flow = 'error';
      
      // The error_flow is the flow of the first value
      // This is questionable but most likely correct
      // A sender's error handler will receive all the values
      error.error_flow = values[ 0 ].flow;
      
      error.operation = 'add';
      
      if ( options && options.sender ) {
        error.sender = options.sender; // to allow routing of error back to sender
      }
      
      error.values = values;
      
      return that.__emit_add( [ error ], options );
    } // emit_error()
  }, // _add()
  
  /* ----------------------------------------------------------------------------------------------
     _remove( values, options )
  */
  _remove: function( values, options ) {
    var that = this
      , emit_values = []
      , vl = values.length
      , key = this._options.key
      , kl = key.length
      , name = de && this._get_name( '_remove' )
      , connection = this._mysql_connection
    ;
    
    if ( vl === 0 || kl === 0 ) return emit(); // propagate options
    
    if ( ! connection ) return this._add_waiter( '_remove', arguments );
    
    // ToDo: handle transactions
    
    // DELETE FROM table WHERE conditions
    
    // Build WHERE conditions based on key
    var escaped_key = key.map( get_escape_column( this, connection ) )
      , where = make_where( this, connection, escaped_key )
      , table = connection.escapeId( this._table )
      , sql = 'DELETE FROM ' + table + where
    ;
    
    de&&ug( name + 'sql:\n\n  ' + sql + '\n' );
    
    // ToDo: in an SQL transaction implemented in a stored procedure read before delete to verify that all deleted values exist
    
    // All added values should have been removed first, the order of operations is important for MySQL
    connection.query( sql, function( error, results ) {
      if ( error ) {
        log( get_name() + 'unable to DELETE FROM', table
          , ', code:'    , error.code
          , ', number:'  , error.errno
          , ', sqlState:', error.sqlState
          , ', index:'   , error.index
          , ', message:' , error.message
          , ', error:'   , error
        );
        
        emit_error( {
          // ToDo: provide toubkal error code from MySQL error
          
          engine: 'mysql',
          
          mysql: {
            table   : that._table,
            code    : error.code,
            number  : error.errno,
            sqlState: error.sqlState,
            index   : error.index,
            message : error.message,
            sql     : sql
          }
        } )
        
        return;
      }
      
      emit_values = values;
      
      // ToDo: if results.affectedRows != values.length, we have a problem
      de&&ug( name + 'deleted rows:', results.affectedRows );
      
      emit(); // valid values
    } )
    
    return this;
    
    function get_escape_column( that, connection ) {
      var columns_aliases = that._columns_aliases;
      
      return escape_column;
      
      function escape_column( a ) {
        var column = columns_aliases[ a ];
        
        if ( column ) return connection.escapeId( column );
        
        throw new Error(
            'key attribute "' + a + '" is not defined in columns (after optional aliasing).'
          + '\n\n  If a column has an alias (the "as" attribute) and is part of key, the alias is the name of the attribute that should be part of key.'
          + '\n\n  key: [ ' + key.join( ', ' ) + ' ]'
          + '\n\n  Columns: ' + JSON.stringify( that._columns, null, ' ' )
          + '\n\n  Aliased columns: ' + JSON.stringify( columns_aliases, null, ' ' )
          + '\n'
        );
      } // escape_column()
    } // get_escape_column()
    
    function make_where( that, connection, escaped_key ) {
      var where = '\n\n  WHERE'
        , parsers = that._parsers
        , i, j, value, a, v, parser
      ;
      
      if ( kl > 1 ) {
        for ( i = -1; ++i < vl; ) {
          value = values[ i ];
          
          if ( i > 0 ) where += '\n     OR';
          
          where += ' (';
          
          for ( j = -1; ++j < kl; ) {
            a = key[ j ];
            v = value[ a ];
            
            if ( v === null || v === undefined ) {
              return emit_error( null_key_attribute_error( i, a, value ) );
            }
            
            if ( parser = parsers[ a ] ) v = parser( v );
            
            where += ( j ? ' AND ' : ' ' )
              + escaped_key[ j ]
              + ' = ' + connection.escape( v )
            ;
          }
          
          where += ' )';
        }
      } else {
        where += ' ' + escaped_key[ 0 ] + ' IN (';
        
        a = key[ 0 ];
        
        parser = parsers[ a ];
        
        for ( i = -1; ++i < vl; ) {
          value = values[ i ];
          v = value[ a ];
          
          if ( v === null || v === undefined ) {
            return emit_error( null_key_attribute_error( i, a, value ) );
          }
          
          if ( parser ) v = parser( v );
          
          where += ( i ? ', ' : ' ' ) + connection.escape( v );
        }
        
        where += ' )';
      }
      
      return where;
    } // make_where()
    
    function emit() {
      return that.__emit_remove( emit_values, options );
    } // emit()
    
    function emit_error( error ) {
      error.flow = 'error';
      
      // The error_flow is the flow of the first value
      // This is questionable but most likely correct
      // A sender's error handler will receive all the values
      error.error_flow = values[ 0 ].flow;
      
      error.operation = 'remove';
      
      if ( options && options.sender ) {
        error.sender = options.sender; // to allow routing of error back to sender
      }
      
      error.values = values;
      
      return that.__emit_add( [ error ], options );
    } // emit_error()
    
    function get_name() {
      that._get_name( '_remove' )
    } // get_name()
  } // _remove()
} } ); // mysql_write()

/* ------------------------------------------------------------------------------------------------
   mysql( table, columns, options )
   
   Parameters:
   - table (String): MySQL table name. The table must exist in MySQL and must have a primary key
     that will be identical to the Pipelet's key unless aliased (see columns definition bellow).
   
   - columns (Array): defines all columns used for SELECT and INSERT, including primary key.
     Each column is defined as:
     - (String): column name
     
     - (Object): all attributes are optional except "id":
       - id (String): MySQL column name
       
       - as (String): dataflow attribute name, default is the value of "id"
       
       - converter: to convert values of this column to/from mysql driver types. For
         more information on further mysql driver type convertions with MySQL types see
         https://www.npmjs.com/package/mysql#type-casting.
         
         A converter can be specified as a string for built-in converters or an Object:
         - (String): a built-in converter, supported converters are:
           - "uuid_b16": converts a UUID to/from MySQL BINARY(16)
         
         - (Object): Providing the following functions:
           - parse     (Function): parse( value ) -> value to mysql driver
           - serialize (Function): serialize( <value from mysql driver> ) -> value
   
   - options (Object): optional attributes:
     - connection (String): id of connection in configuration file, default is 'root'
     
     - configuration (String): filename of configuration file, default is ~/config.rs.json
     
     - mysql (Object): default mysql connection options, see mysql_connections()
     
     - key (Array of Strings): defines the primary key, if key columns are aliased as defined
       above, alliased column names MUST be provided. default is [ 'id' ]
*/
Pipelet.Compose( 'mysql', function( source, table, columns, options ) {
  var connections = rs
    .configuration( { filepath: options.configuration } )
    .filter( [ { id: 'toubkal_mysql#' + ( options.connection || 'root' ) } ] )
    .mysql_connections( { mysql: options.mysql } )
  ;
  
  de&&ug( 'mysql(), options:', options );
  
  var input  = source.mysql_write( table, columns, connections, { name: options.name + '_write', key: options.key } )
    , output = input .mysql_read ( table, columns, connections, { name: options.name + '_read' , key: options.key } )
  ;
  
  return rs.encapsulate( input, output, options );
} ); // mysql()

// toubkal_mysql.js
