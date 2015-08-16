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
  , Set      = RS.Set
  , extend   = RS.extend
  , clone    = extend.clone
  , log      = RS.log.bind( null, 'mysql' )
  , de       = true
  , ug       = de && log
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
   - connection (Pipelet): mysql_connections() output
   - options (optional Object): optional attributes:
     - columns (Array of Strings): default is ['*']
*/
function MySQL_Read( table, connection, options ) {
  var that = this;
  
  connection.greedy()._on( "add", function( connections ) {
    var connection = connections[ 0 ]
      , mysql_connection = connection.mysql_connection
      , columns = options.columns
    ;
    
    columns = columns
      ? columns.map( mysql_connection.escapeId ).join( ', ' )
      : '*'
    ;
    
    table = mysql_connection.escapeId( table );
    
    mysql_connection.query( 'select ' + columns + ' from ' + table, function( error, results, fields ) {
      if ( error ) {
        log( 'Unable to read', table, ', error:', error );
        
        return;
      }
      
      // ToDo: handle remove + add in a transaction
      that.a.length && that._remove( that.a );
      
      that._add( results );
    } )
  } );
  
  Set.call( this, [], options );
} // MySQL_Read()

Set.Build( 'mysql_read', MySQL_Read );

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

RS.Pipelet.Compose( 'mysql', function( source, table, connection, options ) {
  var connections = rs
    .configuration( { filepath: options.configuration } )
    .filter( [ { id: 'toubkal_mysql#' + connection } ] )
    .mysql_connections( { mysql: options.mysql } )
  ;
  
  return rs.mysql_read( table, connections, options );
} );

// toubkal_mysql.js
