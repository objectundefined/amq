var amq = require('../index') ;
var uuid = require('node-uuid') ;
var format = require('util').format ;
var log = console.log.bind(console,'LOG:')
var logErr = console.error.bind(console,'ERROR:')
var connection = amq.createConnection({ host : 'localhost' , debug : true },{ 
	reconnect : { strategy : 'constant' , initial : 1000 } 
});
var when = require('when') ;
var assert = require('assert') ;



describe('exchange', function(){
	var exchange = connection.exchange( format('test-exg-%s',uuid.v4()) , { type : 'direct' , autoDelete : true }) ;
  describe('#bind', function(){
    it('should resolve when calling bind to amq.direct', function(done){
			exchange.bind('amq.direct').then(function(){
				done();
			}).then(null,done)
    })
  })
  describe('#unbind', function(){
    it('should resolve when calling unbind to amq.direct', function(done){
			exchange.unbind('amq.direct').then(function(){
				done();
			}).then(null,done)
    })
  })
  describe('#bind-error', function(){
    it('should reject when calling bind to a random string that isnt a valid exchange', function(done){
			exchange.bind(uuid.v4()).then(function(){
				done(new Error('Should not have bound to a non-existent exchange.'));
			}).then(null,function(){
				done();
			})
    })
  })
})