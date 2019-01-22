###
  mysql.coffee

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

###

# ----------------------------------------------------------------------------------------------
# rs test utils
# -------------

utils  = require './tests_utils.js'

expect = utils.expect
check  = utils.check

# ----------------------------------------------------------------------------------------------
# Require tested modules
# ----------------------

subclass = this.rs && this.rs.RS.subclass

rs = require 'toubkal'

require( '../../toubkal_mysql.js' )( rs )

RS = rs.RS
uuid_v4 = RS.uuid.v4

# ----------------------------------------------------------------------------------------------
# subclass test suite
# -------------------

describe 'mysql()', ->
  input          = null
  users          = null
  mysql_users    = null
  joe            = null
  
  describe 'user table', ->
    it 'should be a Pipelet', ->
      input = rs.events_metadata() # add id as uuid_v4 and timestamp
      
      users = input
        .map( ( user ) -> {
          id         : user.id
          email      : user.email
          login      : user.login
          create_time: user.create_time || user.timestamp
        } )
        
        .set []
      
      mysql_users = users
        .mysql(
          'toubkal_unit_tests.user'
          
          [
            { id: 'id', converter: 'uuid_b16' }
            'email'
            'login'
            'create_time'
          ]
          
          {
            configuration:
              if process.env.TRAVIS
              then './test/fixtures/travis.config.json'
              else null
          }
        )
      
      expect( mysql_users    ).to.be.a RS.Pipelet
    
    it 'should be empty', ( done ) ->
      mysql_users._fetch_all ( users ) ->
        check done, () ->
          expect( users ).to.be.eql []
    
    it 'should allow to add one user', ( done ) ->
      input._add [ {
        email: 'joe@example.com'
        login: 'joe'
      } ]
      
      mysql_users._fetch_all ( _users ) ->
        check done, () ->
          expect( _users.length ).to.be.eql 1
          joe = _users[ 0 ]
          expect( _users ).to.be.eql users._fetch_all()
    
    it 'should allow to fetch a single user using a query', ( done ) ->
      fetched = ( _users ) ->
        check done, () ->
          expect( _users.length ).to.be.eql 1
          expect( _users ).to.be.eql users._fetch_all()
      
      mysql_users._fetch_all fetched, [ { login: 'joe' } ]
    
    it 'should ignore column in query not defined in schema', ( done ) ->
      fetched = ( _users ) ->
        check done, () ->
          expect( _users.length ).to.be.eql 1
      
      mysql_users._fetch_all fetched, [ { _login: 'joe' } ]
    
    it 'should allow to update existing user', ( done ) ->
      input._update [ [
        joe
        
        {
          id         : joe.id
          email      : 'joe@toubkal.rocks'
          login      : 'joe'
          create_time: joe.create_time
        }
      ] ]
      
      mysql_users._fetch_all ( _users ) ->
        check done, () ->
          expect( _users.length ).to.be.eql 1
          
          new_user = _users[ 0 ]
          
          expect( new_user ).to.be.eql {
            id         : joe.id
            email      : 'joe@toubkal.rocks'
            login      : 'joe'
            create_time: joe.create_time
          }
          
          joe = new_user
    
    it 'should allow to remove previously added user', ( done ) ->
      input._remove [ joe ]
      
      mysql_users._fetch_all ( _users ) ->
        check done, () ->
          expect( _users.length ).to.be.eql 0
  
  describe 'user and project table, with foreign key countraint', ->
    it 'should create a project and a user', ( done ) ->
      rs.Singleton( 'database', ( source, options ) ->
        
        return source
          
          .trace( "database in" )
          
          .dispatch( [ { id: "users" }, { id: "projects" } ], ( source, options ) ->
            if this.id == "users"
              source
                .flow( "users" )
                .through( input )
              
              mysql_users
                .set_flow( "users" )
            
            else
              source
                .flow( "projects" )
                
                .mysql(
                  'toubkal_unit_tests.project'
                  
                  [
                    { id: 'id', converter: 'uuid_b16' }
                    { id: 'creator_id', converter: 'uuid_b16' }
                    'name'
                    'create_time'
                  ]
                  
                  {
                    configuration:
                      if process.env.TRAVIS
                      then './test/fixtures/travis.config.json'
                      else null
                  }
                )
                
                .set_flow( "projects" )
          )
      )
      
      jack = null
      
      rs
        .set( [ { name: "Great Project" } ] )
        
        .events_metadata()
        
        .map( ( project ) -> {
          flow       : "projects"
          id         : project.id
          creator_id : project.creator_id
          name       : project.name
          create_time: project.create_time || project.timestamp
        } )
        
        .fetch( mysql_users, ( project ) -> [ { id: project.creator_id } ] )
        
        .map( ( fetched ) ->
          operation = fetched.operation
          user      = fetched.values[ 0 ]
          adds      = []
          
          if operation == "create"
            unless user
              user = jack = { flow: "users", id: uuid_v4(), email: "jack@example.com", login: "jack" }
              fetched.source.creator_id = user.id
              
              adds.push( user )
          
          adds.push( fetched.source )
          
          { adds: adds }
        )
        
        .emit_operations()
        
        .database()
      
      rs
        .once( 20 )
        
        .fetch( rs.database() )
        
        .map( ( fetched ) -> check done, () ->
          fetched = fetched.values
          
          expect( fetched ).to.be.eql [
            {
              flow       : "users"
              id         : jack.id
              email      : "jack@example.com"
              login      : "jack"
              create_time: fetched[ 0 ].create_time
            }
            
            {
              flow       : "projects"
              id         : fetched[ 1 ].id
              creator_id : jack.id
              name       : "Great Project"
              create_time: fetched[ 1 ].create_time
            }
          ]
        )
    
    it 'should remove a project and a user', ( done ) ->
      rs
        .database()
        
        .greedy()
        
        ._output
        
        .once( 'remove', () ->
          
          rs
            .once()
            
            .fetch( rs.database() )
            
            .alter( ( fetched ) -> check done, () ->
              expect( fetched.values ).to.be.eql []
            )
        )
      
      rs
        .once()
        
        .fetch( rs.database() )
        
        .map( ( fetched ) ->
          fetched = fetched.values
          
          { removes: [ fetched[ 1 ], fetched[ 0 ] ] }
        )
        
        .emit_operations()
        
        .database()
  
  describe 'Using spatial geometry fields, WKT geometry', ->
    paris = null
    cities_wkt_output = null
    
    it 'should allow to add Paris in cities_wkt', ( done ) ->
      cities_wkt_output = rs
        .Singleton( 'cities_wkt', ( source, options ) ->
          return source
            
            .flow( 'cities_wkt' )
            
            .mysql(
              'toubkal_unit_tests.cities'
              
              [
                { id: 'id', converter: 'uuid_b16' }
                'name'
                { id: 'geometry', geometry: 'WKT' }
                'create_time'
              ]
              
              {
                configuration:
                  if process.env.TRAVIS
                  then './test/fixtures/travis.config.json'
                  else null
              }
            )
            
            .set_flow( 'cities_wkt' )
        )
        
        .cities_wkt()
        
        .greedy()
        
        ._output
      ;
      
      cities_wkt_output.once( "add", ( values ) -> check done, () ->
        expect( values.length ).to.be 1
        
        value = values[ 0 ]
        
        expect( value ).to.be.eql paris
      )
      
      rs
        .once()
        
        .map( ( _ ) -> paris = {
          flow: "cities_wkt"
          id: uuid_v4()
          name: "Paris"
          geometry: "POINT( 2.349014 48.864716 )"
          create_time: new Date()
        } )
        
        .cities_wkt()
      ;
    
    it "should allow to fetch a city using ST_Distance_Sphere() and 'ST_GeomFromPoint', 2, 48", ( done ) ->
      rs
        .once()
        
        .fetch( rs.cities_wkt(), () ->
          [ { geometry: [
            'ST_Distance_Sphere', [], [
              'ST_GeomFromPoint', 2, 48
            ], '<=', 200000
          ] } ]
        )
        
        ._output.once( "add", ( values ) -> check done, () ->
          fetched_values = values[ 0 ].values
          fetched_value  = fetched_values[ 0 ]
          
          expect( fetched_values.length ).to.be 1
          expect( fetched_value.geometry ).to.be.eql(
            'POINT(2.349014 48.864716)'
          )
        )
      ;
    
    it "should allow to fetch a city using ST_Distance_Sphere() and ST_GeomFromText, 'POINT( 2 48 )', 4326", ( done ) ->
      rs
        .once()
        
        .fetch( rs.cities_wkt(), () ->
          [ { geometry: [
            [ 'ST_Distance_Sphere', [
              'ST_GeomFromText', 'POINT( 2 48 )', 4326
            ] ], '<=', 200000
          ] } ]
        )
        
        ._output.once( "add", ( values ) -> check done, () ->
          fetched_values = values[ 0 ].values
          fetched_value  = fetched_values[ 0 ]
          
          expect( fetched_values.length ).to.be 1
          expect( fetched_value.geometry ).to.be.eql(
            'POINT(2.349014 48.864716)'
          )
        )
      ;
    
    it "should allow to fetch a city using ST_Distance_Sphere() and ST_GeomFromText, 'POINT( 2 48 )' with default 4326 SRID", ( done ) ->
      rs
        .once()
        
        .fetch( rs.cities_wkt(), () ->
          [ { geometry: [
            'ST_Distance_Sphere', [
              'ST_GeomFromText', 'POINT( 2 48 )'
            ], [], '<=', 200000
          ] } ]
        )
        
        ._output.once( "add", ( values ) -> check done, () ->
          fetched_values = values[ 0 ].values
          fetched_value  = fetched_values[ 0 ]
          
          expect( fetched_values.length ).to.be 1
          expect( fetched_value.geometry ).to.be.eql(
            'POINT(2.349014 48.864716)'
          )
        )
      ;
    
    it 'should allow to fetch a city using ST_Distance_Sphere() and ST_GeomFromGeoJSON()', ( done ) ->
      rs
        .once()
        
        .fetch( rs.cities_wkt(), () ->
          [ { geometry: [ "$", 200000, ">=",
            [ 'ST_Distance_Sphere', [
              'ST_GeomFromGeoJSON', { type: "Point", coordinates: [ 2, 48 ] }
            ] ]
          ] } ]
        )
        
        ._output.once( "add", ( values ) -> check done, () ->
          fetched_values = values[ 0 ].values
          fetched_value  = fetched_values[ 0 ]
          
          expect( fetched_values.length ).to.be 1
          expect( fetched_value.geometry ).to.be.eql(
            'POINT(2.349014 48.864716)'
          )
        )
      ;
    
    it 'should  fetch no city if distance is too small using ST_Distance_Sphere() and ST_GeomFromGeoJSON()', ( done ) ->
      rs
        .once()
        
        .fetch( rs.cities_wkt(), () ->
          [ { geometry: [ "$", 20000, ">=",
            [ 'ST_Distance_Sphere', [
              'ST_GeomFromGeoJSON', { type: "Point", coordinates: [ 2, 48 ] }
            ] ]
          ] } ]
        )
        
        ._output.once( "add", ( values ) -> check done, () ->
          fetched_values = values[ 0 ].values
          
          expect( fetched_values.length ).to.be 0
        )
      ;
    
    it 'should allow to remove the city of Paris', ( done ) ->
      cities_wkt_output.once( "remove", ( values ) -> check done, () ->
        expect( values.length ).to.be 1
        expect( values[ 0 ] ).to.be.eql paris
      )
      
      rs
        .once()
        
        .map ( _ ) -> paris
        
        .revert()
        
        .cities_wkt()
      ;
  
  describe 'Using spatial geometry fields, GeoJSON geometry', ->
    paris = null
    cities_geo_json_output = null
    
    it 'should allow to add Paris in cities_geo_json', ( done ) ->
      cities_geo_json_output = rs
        .Singleton( 'cities_geo_json', ( source, options ) ->
          return source
            
            .flow( 'cities_geo_json' )
            
            .mysql(
              'toubkal_unit_tests.cities'
              
              [
                { id: 'id', converter: 'uuid_b16' }
                'name'
                { id: 'geometry', geometry: 'GeoJSON' }
                'create_time'
              ]
              
              {
                configuration:
                  if process.env.TRAVIS
                  then './test/fixtures/travis.config.json'
                  else null
              }
            )
            
            .set_flow( 'cities_geo_json' )
        )
        
        .cities_geo_json()
        
        .greedy()
        
        ._output
      ;
      
      cities_geo_json_output.once( "add", ( values ) -> check done, () ->
        expect( values.length ).to.be 1
        
        value = values[ 0 ]
        
        expect( value ).to.be.eql paris
      )
      
      rs
        .once()
        
        .map( ( _ ) -> paris = {
          flow: "cities_geo_json"
          id: uuid_v4()
          name: "Paris"
          geometry: { type: 'Point', coordinates: [ 2.349014, 48.864716 ] }
          create_time: new Date()
        } )
        
        .cities_geo_json()
      ;
    
    it "should allow to fetch a city using ST_Distance_Sphere() and 'ST_GeomFromPoint', 2, 48", ( done ) ->
      rs
        .once()
        
        .fetch( rs.cities_geo_json(), () ->
          [ { geometry: [
            'ST_Distance_Sphere', [], [
              'ST_GeomFromPoint', 2, 48
            ], '<=', 200000
          ] } ]
        )
        
        ._output.once( "add", ( values ) -> check done, () ->
          fetched_values = values[ 0 ].values
          fetched_value  = fetched_values[ 0 ]
          
          expect( fetched_values.length ).to.be 1
          expect( fetched_value.geometry ).to.be.eql(
            { type: 'Point', coordinates: [ 2.349014, 48.864716 ] }
          )
        )
      ;
    
    it "should allow to fetch a city using ST_Distance_Sphere() and ST_GeomFromText, 'POINT( 2 48 )', 4326", ( done ) ->
      rs
        .once()
        
        .fetch( rs.cities_geo_json(), () ->
          [ { geometry: [
            [ 'ST_Distance_Sphere', [
              'ST_GeomFromText', 'POINT( 2 48 )', 4326
            ] ], '<=', 200000
          ] } ]
        )
        
        ._output.once( "add", ( values ) -> check done, () ->
          fetched_values = values[ 0 ].values
          fetched_value  = fetched_values[ 0 ]
          
          expect( fetched_values.length ).to.be 1
          expect( fetched_value.geometry ).to.be.eql(
            { type: 'Point', coordinates: [ 2.349014, 48.864716 ] }
          )
        )
      ;
    
    it "should allow to fetch a city using ST_Distance_Sphere() and ST_GeomFromText, 'POINT( 2 48 )' with default 4326 SRID", ( done ) ->
      rs
        .once()
        
        .fetch( rs.cities_geo_json(), () ->
          [ { geometry: [
            'ST_Distance_Sphere', [
              'ST_GeomFromText', 'POINT( 2 48 )'
            ], [], '<=', 200000
          ] } ]
        )
        
        ._output.once( "add", ( values ) -> check done, () ->
          fetched_values = values[ 0 ].values
          fetched_value  = fetched_values[ 0 ]
          
          expect( fetched_values.length ).to.be 1
          expect( fetched_value.geometry ).to.be.eql(
            { type: 'Point', coordinates: [ 2.349014, 48.864716 ] }
          )
        )
      ;
    
    it 'should allow to fetch a city using ST_Distance_Sphere() and ST_GeomFromGeoJSON()', ( done ) ->
      rs
        .once()
        
        .fetch( rs.cities_geo_json(), () ->
          [ { geometry: [ "$", 200000, ">=",
            [ 'ST_Distance_Sphere', [
              'ST_GeomFromGeoJSON', { type: "Point", coordinates: [ 2, 48 ] }
            ] ]
          ] } ]
        )
        
        ._output.once( "add", ( values ) -> check done, () ->
          fetched_values = values[ 0 ].values
          fetched_value  = fetched_values[ 0 ]
          
          expect( fetched_values.length ).to.be 1
          expect( fetched_value.geometry ).to.be.eql(
            { type: 'Point', coordinates: [ 2.349014, 48.864716 ] }
          )
        )
      ;
    
    it 'should  fetch no city if distance is too small using ST_Distance_Sphere() and ST_GeomFromGeoJSON()', ( done ) ->
      rs
        .once()
        
        .fetch( rs.cities_wkt(), () ->
          [ { geometry: [ "$", 20000, ">=",
            [ 'ST_Distance_Sphere', [
              'ST_GeomFromGeoJSON', { type: "Point", coordinates: [ 2, 48 ] }
            ] ]
          ] } ]
        )
        
        ._output.once( "add", ( values ) -> check done, () ->
          fetched_values = values[ 0 ].values
          
          expect( fetched_values.length ).to.be 0
        )
      ;
    
    it 'should allow to remove the city of Paris', ( done ) ->
      cities_geo_json_output.once( "remove", ( values ) -> check done, () ->
        expect( values.length ).to.be 1
        expect( values[ 0 ] ).to.be.eql paris
      )
      
      rs
        .once()
        
        .map ( _ ) -> paris
        
        .revert()
        
        .cities_geo_json()
      ;
