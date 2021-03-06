/*  toubkal_mysql.js

    The MIT License (MIT)
    
    Copyright (c) 2015-2016, Reactive Sets
    
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

var mysql     = require( 'mysql' )
  , sqlstring = require( 'sqlstring' )
  , escapeId  = sqlstring.escapeId
  , escape    = sqlstring.escape
;

module.exports = init;

function init( rs ) {

var RS               = rs.RS
  , uuid             = RS.uuid
  , timestamp_string = RS.timestamp_string
  , Pipelet          = RS.Pipelet
  , Greedy           = RS.Greedy
  , Unique           = RS.Unique
  , Query            = RS.Query
  , extend           = RS.extend
  , class_of         = RS.class_of
  , is_array         = RS.is_array
  , clone            = extend.clone
  , object_diff      = RS.object_diff
  , RS_log           = RS.log
  , pretty           = RS_log.pretty
  , log              = RS_log.bind( null, 'mysql' )
  , de               = true
  , ug               = log
  , slice            = Array.prototype.slice
;

/* ----------------------------------------------------------------------------
    mysql_connections_set( options )
    
    Parameters:
    - options (optional Object): optional attributes:
      - mysql (Object): mysql npm module default connection options:
        https://www.npmjs.com/package/mysql#connection-options
*/
function MySQL_Connections_Set( options ) {
  Unique.call( this, [], options );
} // MySQL_Connections_Set()

Unique.Build( 'mysql_connections_set', MySQL_Connections_Set, function( Super ) {
  return {
    _add_value: function( t, connection ) {
      var that          = this
        , identity      = this._identity( connection )
        , mysql_options = extend( {}, this._options.mysql, connection.mysql )
      ;
      
      connection = extend( {}, connection );
      
      connection.mysql = extend( {}, mysql_options );
      
      // Hide paswword, preventing downstream traces from disclosing it in logs
      if( connection.mysql.password ) connection.mysql.password = '***';
      
      // !! Do not trace connection before hiding password to prevent having passwords in traces
      de&&ug( that._get_name( '_add_value' ) + 'adding connection:', identity );
      
      // Transactions are connection-based, shared by all pipelets sharing the same connection
      connection.transactions = {};
      
      connection.connected = false;
      
      // Add value immediately but don't emit anything downstream until connected to MySQL server
      that.__add_value( connection );
      
      t.emit_nothing();
      
      connect( function( error ) {
        if( ! error ) {
          de&&ug( that._get_name( '_add_value' ) + 'Connected to:', identity, connection.mysql );
          
          connection.connected = true;
          
          that.__emit_add( [ connection ] );
        }
      } );
      
      function connect( done ) {
        var mysql_connection = mysql.createConnection( mysql_options );
        
        de&&ug( that._get_name( 'connect' ) + identity, 'mysql:', connection.mysql );
        
        // Try to connect immediately
        mysql_connection.connect( function( error ) {
          if( error ) {
            log( that._get_name( 'connect' ) + 'Warning, while (re)connecting to mysql:', identity, ', error:', error );
            
            return on_error( error, done );
          }
          
          connection.mysql_connection = mysql_connection;
          
          mysql_connection.toJSON = function() { return 'mysql connection' };
          
          done( error );
        } );
        
        mysql_connection.on( 'error', function( error ) {
          log( that._get_name( 'on_error' ) + 'Warning on:', identity, ', error:', error );
          
          connection.connected = false;
          
          // Do not remove in a transaction, as we want to make sure that removal takes immediate effect downstream
          that.__emit_remove( [ connection ] );
          
          on_error( error, _on_error );
          
          function _on_error( error ) {
            if( ! error ) {
              de&&ug( that._get_name( 'on_error' ) + 'Reconnected to:', identity, connection.mysql );
              
              connection.connected = true;
              
              that.__emit_add( [ connection ] );
            }
          } // _on_error()
        } ); // on error
        
        function on_error( error, done ) {
          var timeout;
          
          switch( error.code ) {
            case 'PROTOCOL_CONNECTION_LOST':
            break;
            
            case 'ETIMEDOUT':
              timeout = 2000;
            break;
            
            case 'ECONNREFUSED':
            case 'EHOSTUNREACH':
              timeout = 10000;
            break;
            
            default:
              connection.mysql_connection = null;
              connection.error = error;
              
              log( that._get_name( 'on_error' ) + 'Fatal Error, code:', error.code, ', failed to (re)connect to:', identity, connection.mysql );
              
              done( error );
              
            return;
          }
          
          if( timeout ) {
            connection.set_timeout = setTimeout( try_again, 10000 );
          } else {
            try_again();
          }
          
          function try_again() {
            delete connection.set_timeout;
            
            connect( done );
          }
        } // on_error()
      } // connect()
    }, // _add_value()
    
    _remove_value: function( t, connection ) {
      var i = this._a_index_of( connection );
      
      if( i != -1 ) {
        connection = this.a[ i ];
        
        // ToDo: handle SQL transactions, either by terminating them now or waiting some time for them to terminate
        
        if( connection.mysql_connection ) {
          connection.mysql_connection.destroy();
          
          connection.mysql_connection = null;
        }
        
        de&&ug( this._get_name( '_remove_value' ), i, connection );
        
        if( connection.set_timeout ) {
          // We are in the process of waiting to reconnect to server on a timeout
          clearTimeout( connection.set_timeout );
          
          delete connection.set_timeout;
        }
        
        if( connection.connected ) {
          connection.connected = false;
          
          Super._remove_value.call( this, t, connection );
        } else {
          // We have already emitted remove downstream or never emitted add downstream
          this.__remove_value( connection );
          
          t.emit_nothing();
        }
      } else {
        // This should never happen because this is a unique set
        log( this._get_name( '_remove_value' ) + 'Error removing not found connection:', this._identity( connection ) );
        
        t.emit_nothing();
      }
    } // _remove_value()
  };
} ).singleton(); // mysql_connections_set()

rs.Singleton( 'mysql_connections', function( source, options ) {
  return source
    .optimize( { untag: 'mysql_configuration' } )
    .last()
    .mysql_connections_set( options )
  ;
} );

/* ----------------------------------------------------------------------------
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
  // to mysql
  parse: function( id ) {
    return uuid.parse( id, new Buffer( 16 ) );
  },
  
  // from mysql
  serialize: function( id ) {
    return id && uuid.unparse( id );
  }
} ); // uuid_b16

converters.set( 'timestamp_t3', {
  // to mysql
  parse: function( t ) {
    return t;
  },
  
  // from mysql
  serialize: function( t ) {
    return timestamp_string( new Date( t ) );
  }
} ); // timestamp_t3

converters.set( 'json', {
  // to mysql
  parse: function( json ) {
    var text = JSON.stringify( json );
    
    //de&&ug( 'converter.json parse, JSON.stringify():', text );
    
    return text;
  },
  
  // from mysql
  serialize: function( text ) {
    try {
      var json = JSON.parse( text );
      
      //de&&ug( 'converter.json serialize, JSON.parse():', json );
      
    } catch( e ) {
      // ToDo: emit error
      log( 'JSON.parse error:', e );
      
      json = null;
    }
    
    return json;
  }
} ); // json

converters.set( 'geo_json', extend( {}, converters.get( 'json' ), {
  select: function( column, as ) {
    return "ST_AsGeoJSON( " + column + " ) AS " + ( as || column );
  },
  
  // sql expression to write column to mysql
  write: function( value ) {
    // srid is 4326 by default
    return "ST_GeomFromGeoJSON( " + value + " )";
  }
} ) );

converters.set( 'wkt', {
  // sql expression to read column from mysql
  select: function( column, as ) {
    return "ST_AsText( " + column + " ) AS " + ( as || column );
  },
  
  // sql expression to write column to mysql
  write: function( value ) {
    // force srid to 4326, because default is zero
    return "ST_GeomFromText( " + value + ", 4326 )";
  }
} );

/* ----------------------------------------------------------------------------
    @pipelet mysql_read( table, columns, connection, options )
    
    @parameters
    - **table** (String): mysql table name
    
    - **columns** (Array of Columns): see mysql() for full definition of
      Column.
    
    - **connection** (Pipelet): mysql_connections() output (will use the
      last added).
    
    - **options** (optional Object): optional attributes:
      - **key** (Array of Strings):  field names used to build *WHERE*
        clause for *DELETE*, may be aliased by *columns*.
*/
function MySQL_Read( table, columns, connection, options ) {
  var that = this
    , receivers = []
    , _name
    , parsers = {}
    , serializers = []
    , columns_aliases = []
    , processed_columns = process_columns( columns, parsers, serializers, columns_aliases )
  ;
  
  this._table_escaped = escapeId( table );
  this._mysql_connection = null;
  
  Greedy.call( this, options );
  
  this._add_input(
    connection,
    
    Greedy.Input,
    
    options.name + '-connection',
    
    {
      _add   : add_connections,
      _remove: remove_connections
    }
  );
  
  this._output.source = {
    _fetch: fetch,
    
    update_query_string: function() {}
  };
  
  function process_columns( columns, parsers, serializers, columns_aliases ) {
    var i = -1
      , l = columns.length
      , processed_columns = []
    ;
    
    // !! Do not use columns.map() because it hides user bugs, skipping undefined values
    while( ++i < l ) {
      var column    = columns[ i ]
        , a         = column
        , id
        , as        = null
        , converter = null
        , select    = null
      ;
      
      if ( typeof column === 'object' && column ) {
        id = column.id;
        as = column.as;
        converter = column.converter;
        
        column = id;
        a = as || id;
        
        if ( converter ) {
          converter = converters.get( converter );
          
          if ( converter ) {
            if ( converter.parse ) {
              parsers[ a ] = converter.parse;
            }
            
            if ( converter.serialize ) {
              serializers.push( { id: a, serialize: converter.serialize } );
            }
            
            select = converter.select;
          }
        }
      }
      
      if ( ! column )
        // ToDo: send error to global error dataflow
        throw new Error( 'Undefined column id'
          + ' at position ' + i
          + ' in columns: ' + JSON.stringify( columns )
          + ' of table: '   + table
        )
      ;
      
      columns_aliases[ a ] = column;
      
      // Escape column and as names
      column = escapeId( column );
      
      if ( as ) {
        as = escapeId( as );
      }
      
      if ( select ) {
        column = select( column, as );
      
      } else if ( as ) {
        column += ' AS ' + as;
      
      }
      
      processed_columns.push( column );
    } // while there are columns
    
    return processed_columns.join( '\n       , ' );
  } // process_columns()
  
  function name() {
    return _name || ( _name = that._get_name( 'fetch' ) );
  } // name()
  
  function add_connections( connections ) {
    de&&ug( that._get_name( 'add_connections' ), connections.map( connection_id ), table );
    
    if ( connections.length ) {
      that._mysql_connection = connections[ connections.length - 1 ].mysql_connection;
    }
    
    call_receivers();
  } // add_connections()
  
  function remove_connections( connections ) {
    de&&ug( that._get_name( 'remove_connections' ), connections.map( connection_id ), table );
    
    if( connections.length ) that._mysql_connection = null;
  } // remove_connections()
  
  function call_receivers() {
    while ( that._mysql_connection && receivers.length ) {
      fetch.apply( null, receivers.shift() );
    }
  } // call_receivers()
  
  function add_receiver( _arguments ) {
    de&&ug( that._get_name( 'add_receiver' ), _arguments[ 1 ] );
    
    receivers.push( slice.call( _arguments ) );
  } // add_receiver()
  
  /* --------------------------------------------------------------------------
      fetch( receiver, query )
      
      SELECT values from table, according to query
  */
  function fetch( receiver, query ) {
    var mysql_connection = that._mysql_connection;
    
    if ( ! mysql_connection ) return add_receiver( arguments );
    
    var table = that._table_escaped
      , _name
      , where = ''
    ;
    
    if ( query ) where = where_from_query( query, columns_aliases, parsers );
    
    var sql = '  SELECT ' + processed_columns + '\n\n  FROM ' + table + where;
    
    de&&ug( name() + 'sql:\n\n' + sql + '\n' );
    
    // ToDo: use query.on(), see https://github.com/mysqljs/mysql#streaming-query-rows
    mysql_connection.query( sql, function( error, results, fields ) {
      if ( error ) {
        log( name() + 'unable to read', table, ', error:', error );
        
        switch( error.code ) {
          case 'PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR':
          case 'ER_SERVER_SHUTDOWN':
          case 'PROTOCOL_CONNECTION_LOST':
            // We expect a new connection to execute this query later
            
            add_receiver( [ receiver, query ] );
          break;
          
          default:
            // ToDo: emit out-of-band fatal error
            log( name() + 'table:', table, ', fatal error:', error.code );
            
            receiver( [], true );
          break;
        }
        
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
        
        // de&&ug( name() + 'serialize column:', column, 'with:', serialize );
        
        i = results.length;
        
        while ( i ) {
          result = results[ --i ];
          
          result[ column ] = serialize( result[ column ] );
        } // while there are results
      } // while there are serializers
      
      //de&&ug( name() + 'results:', results.length );
      
      receiver( results, true );
    } )
  } // fetch()
} // MySQL_Read()

function where_from_query( query, columns_aliases, parsers ) {
  var where = query
    .map( function( or_term ) {
      //de&&ug( 'where_from_query(), or_term:', or_term );
      
      or_term = Object.keys( or_term )
        
        .map( function( property ) {
          var value = or_term[ property ]
            , parser
          ;
          
          if ( property === 'flow' && value === 'error' ) {
            return false;
          }
          
          switch ( class_of( value ) ) {
            case 'Number':
            case 'String':
              // scalar values where strict equality is desired
              
              if ( parser = parsers[ property ] ) value = parser( value );
              
              var alias = columns_aliases[ property ];
              
              
              if ( ! alias )
                // ToDo: consider emitting an error because this fetch will effectively emit nothing after filtering
                return false;
              ;
              
              // ToDo: use "property" COLLATE latin1_bin = value, or utf8_bin for case-sensitive comparison
            return escapeId( alias ) + ' = ' + escape( value );
            
            case 'Array': // expression
            return expression_to_sql( escapeId( property ), value );
            
            default:
            // ToDo: emit error
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
  
  function expression_to_sql( property, expression, i ) {
    i = typeof i == 'number' ? i : 0;
    
    /*
      This is work in progress.
      
      examples:
      - expression_to_sql(
          escapeId( 'geometry' ),
          [ [ 'ST_Distance_Sphere', [ 'ST_GeomFromText', 'POINT( 2 48 )' ] ], '<=', 20000 ]
        )
        
        // -> ST_Distance_Sphere( ST_GeomFromText( 'POINT( 2 48 )', 4326 ), `geometry` ) <= 200000
      
      - expression_to_sql(
          escapeId( 'geometry' ),
          [ [ 'ST_Distance_Sphere', [ 'POINT_SRID', 2, 48 ] ], '<=', 20000 ]
        )
        
        // -> ST_Distance_Sphere( ST_GeomFromText( 'POINT( 2 48 )', 4326 ), `geometry` ) <= 200000
      
      - expression_to_sql(
          escapeId( 'geometry' ),
          [ 200000, '>=', [ 'ST_Distance_Sphere', [ 'ST_GeomFromGeoJSON', { type: "Point", coodinates: [ 2, 48 ] } ] ] ]
        )
        
        // -> 200000 >= ST_Distance_Sphere( ST_GeomFromGeoJSON( '{\"type\":\"Point\",\"coordinates\":[2,48]}' ), `geometry` )
    */
    var sql = '';
    
    while ( i < expression.length ) {
      var operator = expression[ i++ ]
        , class_of_operator = class_of( operator )
      ;
      
      if ( class_of_operator == "String" ) {
        switch( operator ) {
          case 'failed':
            // this is like the not operator but applying to previous result
            sql = 'NOT ( ' + ( sql || property ) + ' )';
          break;
          
          case '||':
            sql = '( ' + ( sql || property ) + ' ) OR ( ' + expression_to_sql( property, expression, i ) + ' )';
          break;
          
          case '==':
            operator = '=';
          // fall-through
          case '!=':
          case '>' :
          case '>=':
          case '<' :
          case '<=':
            sql = ( sql || property ) + ' ' + operator + ' '
          // fall-through
          case '$':
            sql += get_parameters( 1 )[ 0 ];
          break;
          
          case 'ST_GeomFromGeoJSON':
            sql += sql_function( operator, get_parameters( 1, [ JSON.stringify ] ) );
          break;
          
          case "ST_PointFromText": // WKT [, SRID = (default) 4326]
          case "ST_GeomFromText" : // WKT [, SRID = (default) 4326]
            sql += sql_function( operator, get_parameters( 2, null, [ null, 4326 ] ) );
          break;
          
          case 'POINT': // Longitude, Latitude
            sql += sql_function( operator, get_parameters( 2 ) );
          break;
          
          case 'ST_GeomFromPoint': // Longitude, Latitude [, SRID = (default) 4326]
            var parameters = get_parameters( 3 )
              , lon_lat    = parameters.slice( 0, 2 )
              , srid       = parameters[ 2 ]
            ;
            
            if ( typeof srid != "number" ) {
              srid = 4326;
            }
            
            sql += sql_function( "ST_GeomFromText",
              [ wkt_function( "POINT", lon_lat ), srid ]
            );
          break;
          
          // Minimum Bounding Rectangle (MBR) functions, that can use spatial indexes
          // These all take two geometry parameters. Default for the second paramenter is `property`
          case "MBRContains"       :
          case "MBRWithin"         :
          case "MBRCoveredBy"      :
          case "MBRCovers"         :
          case "MBRDisjoint"       :
          case "MBREquals"         :
          case "MBRIntersects"     :
          case "MBROverlaps"       :
          case "MBRTouches"        :
          // pass-through
          
          // other geometry comparison functions that do not use spatial indexes
          case "ST_Distance_Sphere": // geometry_a [, geometry_b = (default) property]
            sql += sql_function( operator, get_parameters( 2, null, [ null, property ] ) );
          break;
          
          default:
            // unsuported operator
            error( 'unsupported operator: ' + operator );
          return false;
        } // switch( operator )
      
      } else if ( class_of_operator == 'Array' ) {
        sql += expression_to_sql( property, operator );
      
      } else {
        error( 'operator is a ' + class_of_operator + ', expected a String' );
      }
    } // while there are operators in expression
    
    // In state 1, close parenthesis when number of arguments is incomplete
    // if ( expect && state == 1 ) sql += ends_with;
    
    return sql || property;
    
    function wkt_function( name, parameters ) {
      return escape( name + "( " + parameters.join( " " ) + " )" );
    } // wkt_function()
    
    function sql_function( name, parameters ) {
      return name + "( " + parameters.join( ", " ) + " )";
    } // sql_function()
    
    function get_parameters( count, converters, defaults ) {
      var parameters = []
        , j          = -1
      ;
      
      while ( count-- ) {
        ++j;
        
        if ( i < expression.length ) {
          var parameter   = expression[ i++ ]
            , sub_expression = is_array( parameter )
            , converter = converters && converters[ j ]
          ;
          
          if ( sub_expression ) {
            parameter = expression_to_sql( property, parameter );
          }
          
          if ( converter ) {
            parameter = converter( parameter );
          }
          
          if ( ! sub_expression ) {
            parameter = escape( parameter );
          }
        
        } else if ( defaults && ( parameter = defaults[ j ] ) !== undefined ) {
        } else {
          // no more parameters or default parameters
          break;
        }
        
        parameters.push( parameter );
      } // while ( count-- )
      
      return parameters;
    } // get_parameters()
    
    function error( message ) {
      log( 'error,', message + ', at position', i, 'in expression:', expression );
    }
  } // expression_to_sql()
} // where_from_query()

Greedy.Build( 'mysql_read', MySQL_Read );

/* ----------------------------------------------------------------------------
    @pipelet mysql_write( table, columns, connection, options )
    
    @parameters
    - **table** (String): mysql table name.
    
    - **columns** (Array of Columns): see pipelet mysql() for full
      definition of Column.
    
    - **connection** (Pipelet): pipelet mysql_connections() output
      (will use the last added).
    
    - **options** (Object): optional attributes:
      - **key** (Array of Strings): the set of fileds that uniquely define
        objects and used to build a *WHERE* clause for *DELETE* queries.
        May be aliased by columns.
*/
function MySQL_Write( table, columns, connection, options ) {
  this._table_escaped    = escapeId( table );
  this._columns          = columns;
  this._mysql_connection = null;
  this._waiters          = [];
  
  var column_ids          = this._column_ids          = []
    , aliases             = this._aliases             = []
    , columns_write       = this._columns_write       = []
    , properties_write    = this._properties_write    = {}
    , columns_aliases     = this._columns_aliases     = {}
    , parsers             = this._parsers             = {}
    , that = this
  ;
  
  columns.forEach( add_column );
  
  Greedy.call( this, options );
  
  this._add_input(
    connection,
    
    Greedy.Input,
    
    options.name + '-connection',
    
    {
      _add   : add_connections,
      _remove: remove_connections
    }
  );
  
  // return this; // if called with new
  // return undefined; // if called without new
  
  function add_column( column ) {
    var as        = column
      , id
      , converter
      , write
    ;
    
    if ( typeof column === 'object' ) {
      id = column.id;
      as = column.as || id;
      converter = column.converter;
      column = id;
      
      if ( converter ) {
        converter = converters.get( converter );
        
        if ( converter ) {
          if ( converter.parse ) {
            parsers[ as ] = converter.parse;
          }
          
          write = converter.write;
          
          if ( write ) {
            properties_write[ column ] = write;
          }
        }
      }
    }
    
    columns_write   .push( write    );
    column_ids      .push( column   );
    aliases         .push( as       );
    
    columns_aliases[ as ] = column;
  } // add_column()
  
  function add_connections( connections ) {
    var l = connections.length;
    
    de&&ug( that._get_name( 'add_connections' ), connections.map( connection_id ), table );
    
    if ( l ) {
      that._mysql_connection = connections[ l - 1 ].mysql_connection;
      
      that._call_waiters();
    }
  } // add_connections()
  
  function remove_connections( connections ) {
    de&&ug( that._get_name( 'remove_connections' ), connections.map( connection_id ), table );
    
    if( connections.length ) that._connection = null;
  } // remove_connections()
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
  /* --------------------------------------------------------------------------
      @method MySQL_Write.._add_waiter( method, parameters )
      
      @short Add a MySQL connection waiter for method with parameters
      
      @parameters
      - **method** (String): this instance method name e.g. ```"_add"```
        or ```"_remove"```.
      
      - **parameters** (Array): parameters to call method when MySQL
        connection is ready.
  */
  _add_waiter: function( method, parameters ) {
    var that = this;
    
    de&&ug( this._get_name( '_add_waiter' ) + 'method:' + method );
    
    this._waiters.push( { method: method, parameters: parameters } );
  }, // MySQL_Write.._add_waiter()
  
  /* --------------------------------------------------------------------------
      @method MySQL_Write.._call_waiters()
      
      @short Call connection waiters as long as MySQL connection is ready
  */
  _call_waiters: function() {
    var name = de && this._get_name( '_call_waiter' ) + 'calling method:'
      , waiter
    ;
    
    while( this._mysql_connection && ( waiter = this._waiters.shift() ) ) {
      de&&ug( name, waiter.method );
      
      this[ waiter.method ].apply( this, waiter.parameters );
    }
  }, // MySQL_Write.._call_waiters()
  
  /* --------------------------------------------------------------------------
      @method MySQL_Write.._add( values, options )
      
      @short Add values using SQL *INSERT*
  */
  _add: function( values, options ) {
    var that        = this
      , name        = de && this._get_name( '_add' )
      , emit_values = []
    ;
    
    if ( values.length == 0 ) return emit(); // nothing
    
    var connection = this._mysql_connection;
    
    if ( ! connection ) return this._add_waiter( '_add', arguments );
    
    var column_ids = this._column_ids;
    
    if ( column_ids.length === 0 ) return emit(); // nothing
    
    // ToDo: MySQL_Write.._add(), map Toubkal transactions to MySQL transactions
    
    var bulk_values = make_bulk_insert_list( values, emit_values );
    
    if ( typeof bulk_values !== 'string' ) { // this is an error object
      // ToDo: send error to error dataflow
      // ToDo: add tests for errors
      return emit_error( bulk_values );
    }
    
    var table   = this._table_escaped
      , columns = '\n\n    ( ' + column_ids.map( escape_id ).join( ', ' ) + ' )'
      , sql     = 'INSERT ' + table + columns + bulk_values
    ;
    
    de&&ug( name + 'sql:\n\n  ' + sql + '\n' );
    
    // All added values should have been removed first, the order of operations is important for MySQL
    connection.query( sql, function( error, results ) {
      if ( error ) {
        log( 'Unable to INSERT INTO', table, ', error:', error );
        
        /*
          ToDo: Error Handling:
          - Duplicate key: this should not happen since removes should be done first however:
            - these could be stored in an anti-state if unordered removes are desired
            - this may happen if someone added the conflicting value in the background,
              then consider updating
            - Example:
              { [Error: ER_DUP_ENTRY: Duplicate entry '100000' for key 'PRIMARY'] code: 'ER_DUP_ENTRY', errno: 1062, sqlState: '23000', index: 0 }
          
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
        switch( error.code ) {
          case 'PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR':
          case 'ER_SERVER_SHUTDOWN':
          case 'PROTOCOL_CONNECTION_LOST':
            // We expect a new connection to execute this query later
          return that._add_waiter( '_add', [ values, options ] );
        }
        
        emit_error( {
          // ToDo: provide toubkal error code from MySQL error
          
          engine: 'mysql',
          
          mysql: {
            table   : that._table_escaped,
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
    
    /* ------------------------------------------------------------------------
        make_bulk_insert_list( values, emit_values )
        
        Make bulk insert list and make emit values, limited to actual columns.
        
        That way a read on the table should return the same values as emited
        values.
        
        Missing attributes will be set as null unless part of the key in
        which case an error is returned.
        
        There still may be some discrepencies if columns is not specified
        and some values have undefined columns.
        
        Returns:
          String: bulk_values
          Object: error
    */
    function make_bulk_insert_list( values, emit_values ) {
      var key              = that._options.key
        , columns          = that._aliases
        , parsers          = that._parsers
        , columns_write    = that._columns_write
        , bulk_values      = '\n\n  VALUES\n'
        , vl               = values.length
        , cl               = columns.length
        , i
        , value
        , emit_value
        , c, v, parser
        , write
        , j
      ;
      
      for ( i = -1; ++i < vl; ) {
        value      = values[ i ];
        emit_value = emit_values[ i ] = {};
        
        bulk_values += ( i ? ',\n    ' : '\n    ' );
        
        for ( j = -1; ++j < cl; ) {
          c = columns[ j ];
          v = value[ c ];
          
          emit_value[ c ] = v;
          
          if ( v == null ) { // null or undefined
            if ( key.indexOf( c ) !== -1 ) {
              // this attribute is part of the key, it must be provided
              return null_key_attribute_error( i, c, value );
            }
            
            v = null; // could have been undefined
          } else if ( parser = parsers[ c ] ) {
            v = parser( v );
          }
          
          v = escape( v );
          
          if ( write = columns_write[ j ] ) {
            v = write( v );
          }
          
          bulk_values += ( j ? ', ' : '( ' ) + v;
        }
        
        bulk_values += ' )';
      }
      
      return bulk_values;
    } // make_bulk_insert_list()
    
    function escape_id( id ) {
      return escapeId( id );
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
      
      that.__emit_add( [ error ], options );
    } // emit_error()
  }, // MySQL_Write.._add()
  
  /* --------------------------------------------------------------------------
      @method MySQL_Write.._remove( values, options )
      
      @short Removes values using SQL *DELETE*
  */
  _remove: function( values, options ) {
    var that        = this
      , emit_values = []
      , vl          = values.length
      , key         = this._key
      , kl          = key.length
      , name        = de && get_name()
      , connection  = this._mysql_connection
    ;
    
    // de&&ug( name + 'values:', pretty( values ), '\n  key:', key );
    
    if ( vl == 0 || kl == 0 ) return emit(); // propagate options
    
    if ( ! connection ) return this._add_waiter( '_remove', arguments );
    
    // ToDo: map Toubkal transactions to MySQL transactions
    
    // DELETE FROM table WHERE conditions
    
    // Build WHERE conditions based on key
    var escaped_key = this._escaped_key()
      , where       = make_where( this, escaped_key )
      , table       = this._table_escaped
      , sql         = 'DELETE FROM ' + table + where
    ;
    
    de&&ug( name + 'sql:\n\n  ' + sql + '\n' );
    
    // ToDo: in an SQL transaction implemented in a stored procedure read before delete to verify that all deleted values exist
    
    // All added values should have been removed first, the order of operations is important for MySQL
    connection.query( sql, function( error, results ) {
      if ( error ) {
        log( get_name() + 'unable to DELETE FROM', table, ', error:', error );
        
        switch( error.code ) {
          case 'PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR':
          case 'ER_SERVER_SHUTDOWN':
          case 'PROTOCOL_CONNECTION_LOST':
            // We expect a new connection to execute this query later
          return that._add_waiter( '_add', [ values, options ] );
        }
        
        emit_error( {
          // ToDo: provide toubkal error code from MySQL error
          
          engine: 'mysql',
          
          mysql: {
            table   : that._table_escaped,
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
    
    function make_where( that, escaped_key ) {
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
            
            if ( v == null ) { // null or undefined
              return emit_error( null_key_attribute_error( i, a, value ) );
            } else if ( parser = parsers[ a ] ) {
              v = parser( v );
            }
            
            where += ( j ? ' AND ' : ' ' )
              + escaped_key[ j ]
              + ' = ' + escape( v )
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
          
          if ( v == null ) { // null or undefined
            return emit_error( null_key_attribute_error( i, a, value ) );
          } else if ( parser ) {
            v = parser( v );
          }
          
          where += ( i ? ', ' : ' ' ) + escape( v );
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
      return that._get_name( '_remove' )
    } // get_name()
  }, // MySQL_Write.._remove()
  
  /* --------------------------------------------------------------------------
      @method MySQL_Write.._update( updates, options )
      
      @short Update values using SQL *UPDATE*
      
      @description
      Where condition based on old value.
      
      Only updates columns specified in new values.
      
      Using syntax:
      ```sql
        UPDATE table 
          SET col_name1=expression
          [, SET col_name2=expression]
        WHERE where_condition
      ```
  */
  _update: function( updates, options ) {
    var that         = this
      , emit_updates = []
      , ul           = updates.length
      , key          = this._key
      , kl           = key.length
      , name         = de && get_name()
      , connection   = this._mysql_connection
      , table        = this._table_escaped
      , key_escaped  = this._escaped_key()
    ;
    
    de&&ug( name + 'updates:', pretty( updates ), '\n  key:', key );
    
    if ( ul == 0 || kl == 0 ) return emit(); // propagate options
    
    if ( ! connection ) return this._add_waiter( '_update', arguments );
    
    updates.forEach( update );
    
    function update( update ) {
      // ToDo: MySQL_Write..update(), map Toubkal transactions to MySQL transactions
      
      // UPDATE table SET col=expression WHERE condition
      var set_values = make_set_values( update );
      
      if( set_values.length == 0 ) { // there are no values to update
        return --ul || emit(); // emit valid updates if this was the last one
      }
      
      // Build WHERE conditions based on key
      var sql = 'UPDATE ' + table + ' SET ' + set_values.join( ', ' ) + make_where( key_escaped, update[ 0 ] );
      
      de&&ug( name + 'sql:\n\n  ' + sql + '\n' );
      
      // All added values should have been removed first, the order of operations is important for MySQL
      connection.query( sql, function( error, results ) {
        if ( error ) {
          log( get_name() + 'unable to UPDATE', table, ', error:', error );
          
          switch( error.code ) {
            case 'PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR':
            case 'ER_SERVER_SHUTDOWN':
            case 'PROTOCOL_CONNECTION_LOST':
              // We expect a new connection to execute this query later
            return that._add_waiter( '_add', [ updates, options ] );
          }
          
          emit_error( {
            // ToDo: provide toubkal error code from MySQL error
            
            engine: 'mysql',
            
            mysql: {
              table   : that._table_escaped,
              code    : error.code,
              number  : error.errno,
              sqlState: error.sqlState,
              index   : error.index,
              message : error.message,
              sql     : sql
            }
          } )
          
          return
        } else {
          emit_updates.push( update );
          
          // ToDo: if results.affectedRows != values.length, we have a problem
          de&&ug( name + 'updated rows:', results.affectedRows );
        }
        
        --ul || emit(); // valid updates
      } )
      
      function make_set_values( update, i ) {
        var values              = []
          , parsers             = that._parsers
          , properties_write    = that._properties_write
        ;
        
        object_diff( update[ 0 ], update[ 1 ], set );
        
        return values;
        
        function set( property, added ) {
          var parser   = parsers         [ property ]
            , write    = properties_write[ property ]
            , v
          ;
          
          if ( parser ) added = parser( added );
          
          v = escape( added );
          
          if ( write ) {
            v = write( v );
          }
          
          values.push( escapeId( property ) + ' = ' + v );
        }
      } // make_set_values()
    } // update()
    
    function make_where( key_escaped, value ) {
      var where = '\n\n  WHERE'
        , parsers = that._parsers
        , i, a, v, parser
      ;
      
      if ( kl > 1 ) {
        where += ' (';
        
        for ( i = -1; ++i < kl; ) {
          a = key[ i ];
          v = value[ a ];
          
          if ( v == null ) // null or undefined
            return emit_error( null_key_attribute_error( i, a, value ) );
          
          if ( parser = parsers[ a ] ) v = parser( v );
          
          where += ( i ? ' AND ' : ' ' )
            + key_escaped[ i ]
            + ' = ' + escape( v )
          ;
        }
        
        where += ' )';
      
      } else {
        a = key[ 0 ];
        
        v = value[ a ];
        
        if ( v == null ) // null or undefined
          return emit_error( null_key_attribute_error( 0, a, value ) );
        
        if ( parser = parsers[ a ] ) v = parser( v );
        
        where += ' ' + key_escaped[ 0 ] + ' = ' + escape( v );
      }
      
      return where;
    } // make_where()
    
    function emit() {
      return that.__emit_update( emit_updates, options );
    } // emit()
    
    function emit_error( error ) {
      error.flow = 'error';
      
      // The error_flow is the flow of the first value
      // This is questionable but most likely correct
      // A sender's error handler will receive all the values
      error.error_flow = updates[ 0 ][ 0 ].flow;
      
      error.operation = 'update';
      
      if ( options && options.sender ) {
        error.sender = options.sender; // to allow routing of error back to sender
      }
      
      error.values = updates;
      
      // ToDo: emit in a transaction if there are emit_updates and options is not in an ongoing transaction
      return that.__emit_add( [ error ], options );
    } // emit_error()
    
    function get_name() {
      return that._get_name( '_update' )
    } // get_name()
  }, // MySQL_Write.._update()
  
  _escaped_key: function() {
    var that            = this
      , columns_aliases = this._columns_aliases
    ;
    
    return this._key.map( escape_column );
    
    function escape_column( a ) {
      var column = columns_aliases[ a ];
      
      if ( column ) return escapeId( column );
      
      // ToDo: MySQL_Write.._escaped_key(): emit error instead of throwing
      throw new Error(
          'key attribute "' + a + '" is not defined in columns (after optional aliasing).'
        + '\n\n  If a column has an alias (the "as" attribute) and is part of key, the alias is the name of the attribute that should be part of key.'
        + '\n\n  key: [ ' + key.join( ', ' ) + ' ]'
        + '\n\n  Columns: ' + JSON.stringify( that._columns, null, ' ' )
        + '\n\n  Aliased columns: ' + JSON.stringify( columns_aliases, null, ' ' )
        + '\n'
      );
    } // escape_column()
  } // MySQL_Write.._escaped_key()
} } ); // mysql_write()

// connection_id(): get id from connection Object
function connection_id( connection ) {
  return connection.id;
} // connection_id()

rs.Multiton( 'mysql_configuration',
  function( options ) {
    return '' + options.configuration + '#' + ( options.connection || 'root' ) + '#' + JSON.stringify( options.mysql );
  },
  
  function( source, options ) {
    var connection_terms = [ { id: 'toubkal_mysql#' + ( options.connection || 'root' ) } ];
    
    return source
      .configuration( { filepath: options.configuration } )
      
      /*
        Because mysql_connections() is a singleton, and this is a multiton,
        there may be multiple concurrent inputs to mysql_connections() that
        therefore need to be synchronized.
      */
      //.pass_through( { fork_tag: 'mysql_configuration'  } )
      
      .filter( connection_terms )
      
      .mysql_connections( { mysql: options.mysql } )
      
      .filter( connection_terms )
    ;
  }
); // mysql_configuration()

/* ----------------------------------------------------------------------------
    @pipelet mysql( table, columns, options )
    
    @parameters
    - **table** (String): MySQL table name. The table must exist in MySQL and
      must have a primary key that will be identical to the Pipelet's key
      unless aliased (see columns definition bellow).
    
    - **columns** (Array): defines all columns used for SELECT and INSERT,
      including primary key. Each column is defined as:
      - (String): column name
      
      - (Object): all attributes are optional except "id":
        - **id** (String): MySQL column name.
        
        - **as** (String): dataflow attribute name, default is the value of "id",
        
        - **converter**: to convert values of this column to/from mysql driver
          types. For more information on further mysql driver type convertions
          with MySQL types see
          https://www.npmjs.com/package/mysql#type-casting.
          
          A converter can be specified as a string for built-in converters or
          an Object:
          - (String): a built-in converter, supported converters are:
            - **"uuid_b16"**: converts a UUID to/from MySQL BINARY(16)
          
          - (Object): Providing the following functions:
            - **parse** (Function):
              ```parse( value ) -> value to mysql driver```
            
            - **serialize** (Function):
              ```serialize( <value from mysql driver> ) -> value```
    
    - **options** (Object): optional attributes:
      - **connection** (String): name of connection in configuration file,
        default is ```"root"```.
      
      - **configuration** (String): filename of configuration file, default
        is ```~/config.rs.json```.
      
      - **mysql** (Object): default mysql connection options, see
        @@pipelet:mysql_connections().
      
      - **key** (Array of Strings): defines the primary key, if key columns
        are aliased as defined above, alliased column names MUST be provided.
        default is ```[ 'id' ]```.
*/
rs.Compose( 'mysql', function( source, table, columns, options ) {
  var connection_terms = [ { id: 'toubkal_mysql#' + ( options.connection || 'root' ) } ];
  
  // ToDo: move connections handling outside of this pipelet, as a stateless cache
  // ToDo: handle multiple simultaneous connections for instant HA failover to another master
  // ToDo: handle reads through slave servers, different from masters used for writes
  var connections = rs
    .mysql_configuration( options )
    
    .pass_through()
    
    // mysql_configuration() is a multiton, requires explicit disconnection when source disconnects
    .remove_source_with( source )
  ;
  
  de&&ug( 'mysql(),', table, columns.length, options );
  
  var input  = source.mysql_write( table, columns, connections, { name: options.name + '_write', key: options.key } )
    , output = input .mysql_read ( table, columns, connections, { name: options.name + '_read' , key: options.key } )
  ;
  
  return rs.encapsulate( input, output, options );
} ); // mysql()

} // init()

// toubkal_mysql.js
