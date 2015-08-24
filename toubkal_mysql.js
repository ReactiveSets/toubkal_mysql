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

var mysql    = require( 'mysql' )
  , rs       = require( 'toubkal' )
  , RS       = rs.RS
  , Pipelet  = RS.Pipelet
  , Greedy   = RS.Greedy
  , Set      = RS.Set
  , Query    = RS.Query
  , extend   = RS.extend
  , clone    = extend.clone
  , log      = RS.log.bind( null, 'mysql' )
  , de       = true
  , ug       = de && log
  , slice    = Array.prototype.slice
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
      
      connection.mysql = extend( {}, this._options.mysql, connection.mysql );
      
      var mysql_connection = mysql.createConnection( connection.mysql );
      
      // hide paswword, preventing downstream traces from disclosing it
      connection.mysql.password = connection.mysql.password && '***';
      
      de&&ug( this._get_name( '_add_value' ) + ', mysql:', connection.mysql );
      
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
   mysql_read( table, connection, options )
   
   Parameters:
   - table (String): mysql table name
   - connection (Pipelet): mysql_connections() output (will use the last added)
   - options (optional Object): optional attributes:
     - key (Array of Strings): field names used to build WHERE clause to SELECT added and removed
       values (not currently used)
     - columns (Array of Strings): default is ['*']
   
   ToDo: implement add to read from the table
   ToDo: implement trigger pipelet to pipe changes into process, generating a dataflow of changes to be read
*/
function MySQL_Read( table, connection, options ) {
  var that = this;
  
  this._output || ( this._output = new MySQL_Read.Output( this, 'mysql_read_out' ) );
  
  this._table = table;
  this._mysql_connection = null;
  
  connection
    .greedy()
    ._output
    .on( "remove", remove_connection )
    .on( "add", add_connection )
  ;
  
  Greedy.call( this, [], options );
  
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
  
  function( p, name ) {
    this.receivers = [];
    
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
      
      var options = p._options
        , table = mysql_connection.escapeId( p._table )
        , columns = options.columns
        , where = ''
      ;
      
      columns = columns
        ? columns.map( mysql_connection.escapeId ).join( ', ' )
        : '*'
      ;
      
      if ( query ) where = where_from_query( query, mysql_connection );
      
      var sql = 'SELECT ' + columns + ' FROM ' + table + where;
      
      de&&ug( this._get_name( '_fetch' ) + ', sql:', sql, typeof receiver, query );
      
      mysql_connection.query( sql, function( error, results, fields ) {
        if ( error ) {
          log( 'Unable to read', table, ', error:', error );
          
          return;
        }
        
        if ( query ) {
          results = new Query( query ).generate().filter( results );
        }
        
        receiver( results, true );
      } )
    } // _fetch()
  } // MySQL_Read.Output instance methods
); // MySQL_Read.Output

function where_from_query( query, connection ) {
  var where = query
    .map( function( or_term ) {
      de&&ug( 'where_from_query(), or_term:', or_term );
      
      or_term = Object.keys( or_term )
        
        .map( function( property ) {
          var value = or_term[ property ];
          
          if ( property === 'flow' && value === 'error' ) {
            return false;
          }
          
          switch ( toString.call( value ) ) {
            case '[object Number]':
            case '[object String]':
              // scalar values where strict equality is desired
            return connection.escapeId( property ) + ' = ' + connection.escape( value );
            
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
    
    .join( ') OR (' )
  ;
  
  return where ? ' WHERE ( ' + where + ' )' : '';
  
  function not_empty( v ) { return !!v }
  
  function translate_expression( connection, property, expression ) {
    return false; // work in progress, don't translate to SQL for now
    
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

Greedy.Build( 'mysql_read', MySQL_Read, function( Super ) { return {
  _add: function( values, options ) {
    // ToDo: SELECT table WHERE <build statement to read added values and unquote objects>
    return Super._add.call( this, values, options );
  }, // _add()
} } );

/* ------------------------------------------------------------------------------------------------
   mysql_write( table, connection, options )
   
   Parameters:
   - table (String): mysql table name
   - connection (Pipelet): mysql_connections() output (will use the last added)
   - options (optional Object): optional attributes:
     - key (Array of Strings): the set of fileds that uniquely define objects and used to build
       a WHERE clause for DELETE queries.
       
     - columns (Array of Strings): default will get them from values which may is slower on large
       inserts and is not recommended because it may lead to some discrepencies on inserts where
       some values may not be emitted while they would be emited as null when read from the table.
*/
function MySQL_Write( table, connection, options ) {
  var that = this;
  
  this._table = table;
  this._mysql_connection = null;
  this._waiters = [];
  
  connection
    .greedy()
    ._output
    .on( "add", add_connection )
    .on( "remove", remove_connection )
  ;
  
  Greedy.call( this, options );
  
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
     _get_columns( values )
     
     Get columns from:
       - if options.columns is set from options.key and options.columns
       - otherwise from options.key and values
     .
     
     Attributes flow and _v are ignored from options.key and values, but are accepted from
     options.columns.
  */
  _get_columns: function( values ) {
    var options = this._options
      , key = options.key.filter( function( p ) { return p !== 'flow' && p !== '_v'; } )
      , columns = options.columns
    ;
    
    if ( columns ) {
      // Add key attributes to columns if not present, they should!
      
      // Make a copy of columns to prevent alteration
      columns = slice.call( columns );
      
      key.forEach( function( p ) {
        if ( columns.indexOf( p ) === -1 ) columns.unshift( p );
      } );
      
    } else {
      // Find columns from key and values.
      var keys = {};
      
      // Add key columns as properties of keys, set to true
      key.forEach( function( p ) { keys[ p ] = true } );
      
      var vl = values.length;
      
      for ( var i = -1; ++i < vl; ) {
        var value = values[ i ];
        
        for ( var p in value ) {
          keys[ p ] || value.hasOwnProperty( p ) && ( keys[ p ] = true );
        }
      }
      
      // Remove flow and _v attributes if added
      delete keys[ 'flow' ];
      delete keys[ '_v' ];
      
      columns = Object.keys( keys );
    }
    
    return columns;
  }, // _get_columns()
  
  /* ----------------------------------------------------------------------------------------------
     _add( values, options )
  */
  _add: function( values, options ) {
    var emit_values = [];
    
    if ( values.length === 0 ) return emit(); // nothing
    
    var that = this
      , name
      , connection = this._mysql_connection
    ;
    
    if ( ! connection ) return this._add_waiter( '_add', arguments );
    
    var columns = this._get_columns( values );
    
    if ( columns.length === 0 ) return emit(); // nothing
    
    var bulk_values = make_bulk_insert_list( this._options.key, columns, values, emit_values );
    
    if ( typeof bulk_values !== 'string' ) { // this is an error object
      // ToDo: send error to error dataflow
      emit_error( bulk_values );
      
      return this;
    }
    
    columns = '\n  (' + columns.map( connection.escapeId ).join( ', ' ) + ')';
    
    var table = connection.escapeId( this._table )
      , sql = 'INSERT ' + table + columns + bulk_values
    ;
    
    de&&ug( this._get_name( '_add' ) + 'sql:', sql );
    
    // All added values should have been removed first, the order of operations is important for MySQL
    connection.query( sql, function( error, results, fields ) {
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
      
      emit(); // valid values
    } )
    
    return this;
    
    /* --------------------------------------------------------------------------------------------
       make_bulk_insert_list( key, columns, values, emit_values )
       
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
    function make_bulk_insert_list( key, columns, values, emit_values ) {
      var bulk_values = '\nVALUES'
        , vl = values.length
        , cl = columns.length
      ;
      
      for ( var i = -1; ++i < vl; ) {
        var value = values[ i ]
          , emit_value = emit_values[ i ] = {}
          , c, v
        ;
        
        bulk_values += ( i ? ',\n  ' : '\n  ' );
        
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
          
          bulk_values += ( j ? ', ' : '( ' ) + connection.escape( emit_value[ c ] = v );
        }
        
        bulk_values += ' )';
      }
      
      return bulk_values;
    } // make_bulk_insert_list()
    
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
    
    function get_name() {
      return name || ( name = that._get_name( '_add' ) );
    } // get_name()
  }, // _add()
  
  /* ----------------------------------------------------------------------------------------------
     _remove( values, options )
  */
  _remove: function( values, options ) {
    var emit_values = []
      , vl = values.length
      , key = this._options.key
      , kl = key.length
    ;
    
    if ( vl === 0 || kl === 0 ) return emit();
    
    var that = this, name;
    
    var connection = this._mysql_connection;
    
    if ( ! connection ) return this._add_waiter( '_remove', arguments );
    
    // DELETE FROM table WHERE conditions
    
    // Build conditions based on key
    var where = ' WHERE'
      , i, j, value, a, v
    ;
    
    if ( kl > 1 ) {
      for ( i = -1; ++i < vl; ) {
        value = values[ i ];
        
        if ( i > 0 ) where += ' OR';
        
        where += ' (';
        
        for ( j = -1; ++j < kl; ) {
          a = key[ j ];
          v = value[ a ];
          
          if ( v === null || v === undefined ) {
            return emit_error( null_key_attribute_error( i, a, value ) );
          }
          
          where += ( j ? ' AND ' : ' ' ) + connection.escapeId( a ) + ' = ' + connection.escape( v );
        }
        
        where += ' )';
      }
    } else {
      a = key[ 0 ];
      
      where += ' ' + connection.escapeId( a ) + ' IN (';
      
      for ( i = -1; ++i < vl; ) {
        value = values[ i ];
        v = value[ a ];
        
        if ( v === null || v === undefined ) {
          return emit_error( null_key_attribute_error( i, a, value ) );
        }
        
        where += ( i ? ', ' : ' ' ) + connection.escape( v );
      }
      
      where += ' )';
    }
    
    var table = connection.escapeId( this._table )
      , sql = 'DELETE FROM ' + table + where
    ;
    
    de&&ug( this._get_name( '_remove' ) + 'sql:', sql );
    
    // All added values should have been removed first, the order of operations is important for MySQL
    connection.query( sql, function( error, results, fields ) {
      if ( error ) {
        log( 'Unable to DELETE FROM', table
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
      
      emit(); // valid values
    } )
    
    return this;
    
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
      return name || ( name = that._get_name( '_remove' ) );
    } // get_name()
  } // _remove()
} } ); // mysql_write()

/* ------------------------------------------------------------------------------------------------
   mysql( table, connection, options )
   
   Parameters:
   - table (String): mysql table name
   - connection (String): id of connection in configuration file, e.g. 'root'
   - options (optional Object): optional attributes:
     - configuration (String): filename of configuration file, default is ~/config.rs.json
     - mysql (Object): default mysql connection options, see mysql_connections()
     - columns (String or Array of Strings): default is '*'
*/
require( 'toubkal/lib/server/file.js' ); // for configuration()

Pipelet.Compose( 'mysql', function( source, table, connection, options ) {
  var connections = rs
    .configuration( { filepath: options.configuration } )
    .filter( [ { id: 'toubkal_mysql#' + connection } ] )
    .mysql_connections( { mysql: options.mysql } )
  ;
  
  var writer = source.mysql_write( table, connections, options )
    , reader = writer.mysql_read( table, connections, options )
  ;
  
  return rs.encapsulate( writer, reader, options );
} );

// toubkal_mysql.js
