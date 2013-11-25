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

/**
 * 
	ChannelModel#close
*	ChannelModel#on('close', function() {...})
*	ChannelModel#on('error', function (err) {...})
	ChannelModel#createChannel()
	Channel#close()
	Channel#on('close', function() {...})
*	Channel#on('error', function(err) {...})
*	Channel#on('return', function(msg) {...})
*	Channel#on('drain', function() {...})
*	Channel#assertQueue([queue], [options])
*	Channel#checkQueue(queue)
*	Channel#deleteQueue(queue)
*	Channel#purgeQueue(queue)
*	Channel#bindQueue(queue, source, pattern, [args])
*	Channel#unbindQueue(queue, source, pattern, [args])
*	Channel#assertExchange(exchange, type, [options])
	Channel#checkExchange(exchange)
	Channel#deleteExchange(name, [options])
*	Channel#bindExchange(destination, source, pattern, [args])
*	Channel#unbindExchange(destination, source, pattern, [args])
*	Channel#publish(exchange, routingKey, content, [options])
*	Channel#sendToQueue(queue, content, [options])
*	Channel#consume(queue, callback, [options])
	Channel#cancel(consumerTag)
*	Channel#get(queue, [options])
*	Channel#ack(message, [allUpTo])
*	Channel#ackAll()
*	Channel#nack(message, [allUpTo], [requeue])
*	Channel#nackAll([requeue])
*	Channel#prefetch(count)
*	Channel#recover()
 */

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

describe('queue', function(){
	var queue = connection.queue( '' , { exclusive : true }) ;
  describe('#bind', function(){
    it('should resolve when calling bind to amq.direct', function(done){
			queue.bind('amq.direct').then(function(){
				done();
			}).then(null,done)
    })
  })
  describe('#unbind', function(){
    it('should resolve when calling bind to amq.direct', function(done){
			queue.unbind('amq.direct').then(function(){
				done();
			}).then(null,done)
    })
  })
  describe('#consume', function(){
    it('should get a message when subscribing to this queue and sending directly to it.', function(done){
			var queueName = 'subCheck-'+uuid();
			var queue = connection.queue( queueName , { exclusive : true }) ;
			var fn = function ( message ) { 
				assert.equal(message.content.toString('utf8') , 'foo' , 'Incorrect Message Received' );
				queue.ack(message);
				done();
			}
			queue.consume(fn).then(function(subscription){
				return connection.sendToQueue(queueName,'foo')
			}).then(null,done)
    })
  })

  describe('#consume-prefetch', function(){
    it('should only get one message at a time.', function(done){
			var queueName = 'subPrefCheck-'+uuid();
			var queue = connection.queue( queueName , { exclusive : true , prefetch : 1 }) ;
			var amt = 5;
			var recvd = 0;
			var handler = function ( message ) { 
				recvd++;
				queue.check().then(function(queueInfo){
					assert.equal(queueInfo.messageCount , (amt-recvd) , 'Incorrect number of queued messages pending.' );
					return queue.ack(message);
				}).then(function(){
					if ( recvd == amt ) done();
				}).then(null,done)

			};
			
			queue.consume(handler).then(null,done); // trap errors and send through done fn

			for ( var i = 0 ; i < amt ; i++ ) queue.publish(i).then(null,done) ;

    })
  })

  describe('#consume-no-prefetch', function(){
    it('should get all messages available without prefetch set.', function(done){
			var queueName = 'subNoPrefCheck-'+uuid();
			var queue = connection.queue( queueName , { exclusive : true }) ;
			var amt = 5;
			var recvd = 0;
			var handler = function ( message ) { 
				queue.check().then(function(queueInfo){
					assert.equal(queueInfo.messageCount , 0 , 'No messages should be pending in the queue.' );
					if ( ++recvd == amt ) done();
				}).then(null,done)
			};
			
			queue.consume(handler).then(null,done); // trap errors and send through done fn
			
			for ( var i = 0 ; i < amt ; i++ ) queue.publish(i).then(null,done) ;

    })
  })
	
  describe('#consume/#cancel', function(){
    it('should stop getting messages when i cancel a consumer.', function(done){
			var queueName = 'cancelConsumerChk-'+uuid();
			var queue = connection.queue( queueName , { exclusive : true }) ;
			var sendMessage = queue.publish.bind(queue,'message');
			var stopAmt = null ;
			var recvd = 0 ;

			queue.consume(function(){
				recvd++ ;
				setTimeout(sendMessage,1);
			}).then(function(consumer){
				var tag = consumer.consumerTag ;
				sendMessage();
				setTimeout(function(){
					queue.cancel(tag).then(function(){
						stopAmt = recvd ;
						setTimeout(function(){
							if ( recvd > stopAmt ) done(new Error('Messages received after cancel called.'));
							else done()
						},5)
					});
				},10)
			}).then(null,done);
    })
  })
	
  describe('#get', function(){
    it('should get a message from this queue after sending directly to it.', function(done){
			var queueName = 'getCheck-'+uuid();
			var queue = connection.queue( queueName , { exclusive : true }) ;
			queue.publish('foo').then(function(){
				queue.get(queueName).then(function(message){
					assert.equal( message.content.toString('utf8'), 'foo' , 'Incorrect Message Received' );
					queue.ack(message);
					done();
				},done)
			}).then(null,done);
    })
  })
  describe('#ack', function(){
    it('should reduce message ct from 1 to 0 after getting and acking.', function(done){
			var queueName = 'ackCheck-'+uuid();
			var queue = connection.queue( queueName , { exclusive : true , confirm : true });
			var firstPass = false ;
			queue.publish('foo',{},function(){
				// use confirm to know that the message is available
				queue.check().then(function(d){
					assert.equal(d.messageCount,1,'Message Count');
					return queue.get(queueName);
				}).then(function(message){
					assert.equal(message.content.toString('utf8') , 'foo' , 'Incorrect Message Received' );
					return queue.ack(message);
				}).then(function(){
					return queue.check();
				}).then(function(d){
					assert.equal(d.messageCount,0,'Message Count');
					assert.equal(d.consumerCount,0,'Consumer Count');
					done();
				}).then(null,done);
			}).then(null,done);
    })
  })
  describe('#destroy', function(){
    it('Destroy succeeds but checkQueue fails afterward.', function(done){
			var queue = connection.queue( '' , { exclusive : true }) ;
			queue.destroy().then(function(){
				queue.check().then(null,assert.ifError).then(null,done.bind(null,null));
			}).then(null,done)
    })
  })

  describe('#destroy-twice', function(){
    it('Destroy succeeds once but not twice.', function(done){
			var queue = connection.queue( '' , { exclusive : true }) ;
			queue.destroy().then(function(){
				return queue.destroy().then(function(){
					return done(new Error('Should not have destroyed a second time'))
				}).then(null,function(e){ 
					done();
				})
			}).then(null,done)
    })
  })

  describe('#confirm', function(){
    it('should confirm a message sent and received through this queue in confirm mode.', function(done){
			var queue = connection.queue( '' , { exclusive : true , confirm : true }) ;
			return queue.publish('foo',{}, done );
    })
  })
})