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

rs = require '../../toubkal_mysql.js'
RS = rs.RS

# ----------------------------------------------------------------------------------------------
# subclass test suite
# -------------------

describe 'mysql()', ->
  console.log 'describe'
  
  describe 'mysql( user, connection )', ->
    input       = null
    users       = null
    mysql_users = null
    joe         = null
    
    it 'should be a Pipelet', ->
      input = rs.events_metadata() # add id as uuid_v4 and timestamp
      
      users = input
        .map( ( ( user ) -> return {
          id         : user.id
          email      : user.email
          login      : user.login
          create_time: user.timestamp
        } ) )
        
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
      
      expect( mysql_users ).to.be.a RS.Pipelet
    
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
      
      mysql_users._output._fetch fetched, [ { login: 'joe' } ]
    
    it 'should throw an Error when querying a column not defined in schema', ( done ) ->
      fetched = ( _users ) ->
        expect( _users.length ).to.be.eql 1
        expect( _users ).to.be.eql users._fetch_all()
      
      fetch = () ->
        mysql_users._output._fetch fetched, [ { _login: 'joe' } ]
      
      expect( fetch ).to.throwException()
      done()
      
    it 'should allow to remove previously added user', ( done ) ->
      input._remove [ joe ]
      
      mysql_users._fetch_all ( _users ) ->
        check done, () ->
          expect( _users.length ).to.be.eql 0
