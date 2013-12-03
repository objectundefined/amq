var amq = require('../index') ;
var uuid = require('node-uuid') ;
var format = require('util').format ;
var log = console.log.bind(console,'LOG:')
var logErr = function ( e ) { console.error('ERROR',e.stack||e) };
var connection = amq.createConnection({ host : 'localhost' , debug : true },{ 
	reconnect : { strategy : 'constant' , initial : 1000 } 
});
var when = require('when') ;
var assert = require('assert') ;

describe('rpc', function(){
	var rpc = connection.rpc() ;
  describe('#expose-str/resolve', function(){
    it('should create a queue, resolve messages', function(done){
			rpc.expose( 'rpc.foobar' , function(data){
				return when('foo')
			}).then(function(consumer){
				rpc.call( 'rpc.foobar' , null ).then(function(res){
					assert(res=='foo','RPC result is not \'foo\'')
					done();
				}).then(null,done)
			}).then(null,done);
    })
  })
  describe('#expose-str/resolve/awaitReply=false', function(){
    it('should create a queue, resolve messages', function(done){
			rpc.expose( 'rpc.foobar' , function(data){
				return when('foo')
			}).then(function(consumer){
				rpc.call( 'rpc.foobar' , null , { awaitReply : false } ).then(function(res){
					/*published returns true only when expectReply==false*/
					assert(res!=='foo'&&res==true,'RPC result should be \'true\' because awaitReply is false')
					done();
				}).then(null,done)
			}).then(null,done);
    })
  })
  describe('#expose-q/reject', function(){
    var queueName = 'rpcCheck-'+uuid();
		var q = connection.queue(queueName,{autoDelete:true});
		it('should create a queue, resolve messages', function(done){
			rpc.expose( q , function(data){
				return when.promise(function(resolve,reject){
					reject('bar')
				})
			}).then(function(consumer){
				rpc.call( q.name , null ).then(function(res){
					done(new Error('Should Not Resolve'));
				}).then(null,function(err){
					assert(err=='bar','Error was not \'bar\'')
					done();
				})
			}).then(null,done);
    })
  })
  describe('#expose-q/notify', function(){
    var queueName = 'rpcCheck-'+uuid();
		var q = connection.queue(queueName,{autoDelete:true});
		it('should create a queue, notify twice, then resolve', function(done){
			rpc.expose( q , function(data){
				return when.promise(function(resolve,reject,notify){
					setTimeout(function(){
						notify(1);
					},5);
					setTimeout(function(){
						notify(2);
					},10);
					setTimeout(function(){
						resolve(3);
					},15);
				})
			}).then(function(consumer){
				var notifyResults = [];
				rpc.call( q.name , null ).then(function(res){
					assert(notifyResults.indexOf(1)==0,'Was not notified in order')
					assert(notifyResults.indexOf(2)==1,'Was not notified in order')
					assert(res==3,'Was not resolved in order')
					done()
				}).then(null,null,function onNotify(note){
					notifyResults.push(note)
				}).then(null,done)
			}).then(null,done);
    })
  })

})