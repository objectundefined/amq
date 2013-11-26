var events = require('events') ;
var util = require('util') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var format = require('util').format ;
var recant = require('recant') ;
var uuid = require('node-uuid') ;
var Exchange = require('./exchange') ;
var Queue = require('./Queue') ;

util.inherits( Rpc , events.EventEmitter ) ;

module.exports = Rpc ;

function Rpc ( connection , exchange ) {
	
	var self = this ;
	var replyTo = uuid() ;
	
	Object.defineProperty(self,'_connection', { value: connection });
	Object.defineProperty(self,'_exchange', { 
		value: exchange instanceof Exchange ? exchange : connection.exchange('amq.direct') 
	});
	Object.defineProperty(self,'_replyQueue', { 
		value: connection.queue( replyTo , { exclusive : true })
	});
	// we're using this eventEmitter to subscribe to promise-resolving states, 
	// so we'll quickly initialize more than 10 listeners under load. Should
	// this be configurable? I'm not so sure...
	self.setMaxListeners(0);
	self._exchange.on('return',function(d){
		var cid = d.properties.correlationId ;
		self.emit( cid , 'reject' , new Error('Unroutable: queue/routing-key not bound to exchange') );
	});
	self._replyQueue.consume({ exclusive : true , noAck : true },function(reply){
		var cid = reply.properties.correlationId;
		var replyType = reply.prototype.messageId;
		var data;
		try { data = JSON.parse(reply.content) } catch(e) { data = reply.content.toString('utf8') };
		self.emit( cid , replyType , data );
	})
		
};

Rpc.prototype.ready = function(){
	
	var self = this ;
	
	return self._replyQueue.ready().then(function(){
		return self._exchange.ready().then(function(){
			return when({ replyQueue : self._replyQueue , exchange : self._exchange })
		})
	})
	
};

Rpc.prototype.call = function ( routingKey , args , options ) {
	
	var self = this ;
	var cid = uuid.v4(); // more unique b/ not timestamp based
	
	return self.ready().then(function(d){
		var replyQueue = d.replyQueue.name;
		var exchange = d.exchange;
		var d = ( typeof args !== undefined ) ? args : null ;
		var message = JSON.strinigify(d);
		var opts = _.defaults( options || {} , { 
			mandatory : true , 
			contentType : 'application/json' ,
			correlationId : cid ,
			replyTo : replyQueue ,
			timestamp : Date.now() ,
			type : 'rpc'
		});
		
		return d.exchange.publish( routingKey , message , opts ).then(function(){
			
			return when.promise(function(resolve,reject,notify){
				
				var handleReplies = function( replyType , data ){
					var handlers = { 'resolve' : resolve , 'reject' : reject , 'notify' : notify }
					if ( 'resolve' == replyType || 'reject' == replyType ) {
						self.removeListener( cid , handleReplies );
					}
					handlers[ replyType ]( data );
				};
				
				self.on( cid , handleReplies );
				
			})
			
		})
		
	})
	
};

Rpc.prototype.expose = function ( q , opts , fn ) {
	
	return when.promise(function(resolve,reject,notify){
		
		var queue;
		var handler;
		var options;
		
		if ( q instanceof Queue ) {
			queue = q ;
		} else if ( typeof q == 'string' ) {
			queue = self._connection.queue(q);
		} else {
			throw new Error('Invalid Queue/Method-Signature Supplied');
		}
		
		if ( typeof opts == 'object' ) {
			options = opts ;
		} else if ( typeof opts == 'function' ) {
			handler = opts ;
			options = {};
		} else {
			options = {};
		}
		
		if ( !handler && typeof fn == 'function' ) {
			handler = fn ;
		} else {
			throw new Error('Supplied Method Handler is not a function.')
		}
		
		queue.ready().then(function( d ){
			
			return queue.bind(self._exchange)
		
		}).then(function(){
			
			return queue.consume(opts,function(message){
				var doAck = !options.noAck ;
				var ack = queue.ack.bind(queue,message) ;
			
				var prom = when.promise(function (resolve, reject, notify) {
					var isJson = message.properties.contentType == 'application/json' ;
					var data = isJson ? JSON.parse(message.content) : message.content ;
					handler( data , resolve , reject , notify ) ;
				});
			
				if ( message.replyTo && message.correlationId ) {
					prom.then(function(data){
						self.sendReply( 'resolve' ,message.replyTo , message.correlationId , data );
					}).then(null,function(error){
						var log = format('%s Error [rpc]:\n%s', queue.name , error.stack||error );
						self._connection.emit('info', log );
						self.sendReply( 'reject', message.replyTo , message.correlationId , error );
					}).then(null,null,function(notification){
						self.sendReply( 'notify' , message.replyTo , message.correlationId , notification );
					});
				}
			
				if ( doAck ) {
					prom.then(ack,ack);
				}
				
			})
			
		}).then(resolve,reject)
		
	})
		
};


Rpc.prototype.sendReply = function ( replyType , replyTo , correlationId , data ) {
	
	var self = this ;
	var options = { contentType: 'application/json' , correlationId: correlationId , messageId: replyType };
	var message = util.isError(data) ? { 'message' : err.message , 'stack' : err.stack } : data ;

	
	return self._connection.sendToQueue( replyTo , JSON.stringify(message) , options , callback )
	
}