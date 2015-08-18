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
  
  Pipelet.call( this, options );

  function add_connection( connections ) {
    that._connection = connections[ connections.length - 1 ].mysql_connection;
    
    // Call waiters
    that._waiters.forEach( function( waiter ) { waiter() } );
    
    that._waiters = [];
  } // add_connection()
  
  function remove_connection() {
    that._connection = null;
  } // remove_connection()
} // MySQL_Write()

Pipelet.Build( 'mysql_write', MySQL_Write, function( Super ) { return {
  _add_waiter: function( method, parameters ) {
    var that = this;
    
    this._waiters.push( function() { that[ method ].apply( that, parameters ) } );
    
    return this;
  }, // _add_waiter()
  
  _get_columns: function( values ) {
    return this._options.columns || columns_from_values( values );
    
    function columns_from_values( values ) {
      var keys = {}, vl = values.length;
      
      for ( var i = -1; ++i < vl; ) {
        var value = values[ i ];
        
        for ( var p in value )
          if ( p !== 'flow' && p !== '_v' && value.hasOwnProperty( p ) )
            keys[ p ] = true;
      };
      
      return Object.keys( keys );
    } // columns_from_values()
  }, // _get_columns()
  
  _add: function( values, options ) {
    var connection = this._mysql_connection;
    
    if ( ! connection ) return this._add_waiter( '_add', [ values, options ] );
    
    var emit_values = []
      , vl = values.length
    ;
    
    if ( vl === 0 ) return emit();
    
    var columns = this._get_columns( values )
      , cl = columns.length
    ;
    
    if ( cl === 0 ) return emit();
    
    // Make bulk insert list and make emit values, limited to actual columns
    // That way a read on the table should return the same values as emited values
    // Attributes such as "flow" and "_v" will not be emitted unless explicity defined in options.columns
    // Attributes present in some values but not others will be set as null
    // There still may be some discrepencies if options.columns is not specified and some values have undefined columns
    var bulk_values = '\nVALUES'
      , escape = connection.escape
    ;
    
    for ( var i = -1; ++i < vl; ) {
      var value = values[ i ]
        , emit_value = emit_values[ i ] = {}
        , c = columns[ 0 ]
      ;
      
      // there is at least one column
      bulk_values += ( i ? ',\n  ( ' : '\n  ( ' ) + escape( emit_value[ c ] = value[ c ] || null );
      
      for ( var j = 0; ++j < cl; ) {
        c = columns[ j ];
        
        bulk_values += ', ' + escape( emit_value[ c ] = value[ c ] || null );
      }
      
      bulk_values += ' )';
    }
    
    columns = '\n  (' + columns.map( connection.escapeId ).join( ', ' ) + ')';
    
    var table = connection.escapeId( this._table );
    
    // All added values should have been removed first, the order of operations is important for MySQL
    connection.query( 'INSERT ' + table + columns + bulk_values, function( error, results, fields ) {
      if ( error ) {
        log( 'Unable to insert into', table, ', error:', error );
        
        // ToDo: Handle errors:
        // - Duplicate key: this should not happen since removes should be done first however:
        //   - these could be stored in an anti-state if unordered removes are desired
        //   - this may happen if someone added the conflicting value in the background,
        //     then consider updating
        // - Connection error:
        //   - should atempt to reconnect, if it fails continuously, then the application may not
        //     be able to function, consider terminating the process
        // - Constraints violations:
        //   - consider terminating the process
        
        return;
      }
      
      emit();
    } )
    
    return this;
    
    function emit() {
      return that._emit_add( emit_values, options );
    } // emit()
  }, // _add()
  
  _remove: function( values, options ) {
    var connection = this._mysql_connection;
    
    if ( ! connection ) return this._add_waiter( '_remove', [ values, options ] );
    
    // Delete values from table
    
    return this;
  }
} } );

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
  
  return source
    .mysql_write( table, connections, options )
    .mysql_read( table, connections, options )
  ;
} );

// toubkal_mysql.js
