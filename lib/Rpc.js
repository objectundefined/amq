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
var msgpack = require('msgpack-js') ;

util.inherits( Rpc , events.EventEmitter ) ;

module.exports = Rpc ;

function Rpc ( connection , exchange ) {
	
	var self = this ;
	var replyTo = 'rpc_consumer.' + uuid() ;
	
	Object.defineProperty(self,'_connection', { value: connection });
	Object.defineProperty(self,'_exchange', { 
		value: exchange instanceof Exchange ? exchange : connection.exchange('amq.direct') 
	});
	Object.defineProperty(self,'_replyQueue', { 
		value: connection.queue( replyTo , { exclusive : true })
	});
	self._exchange.on('return',function(d){
		var cid = d.properties.correlationId ;
		self.emitReply( cid , 'reject' , new Error('Unroutable: queue/routing-key not bound to exchange') );
	});
	self._consumerPromise = self._replyQueue.consume({ exclusive : true , noAck : true },function(reply){
		var cid = reply.properties.correlationId;
		var replyType = reply.properties.messageId;
		var data;
		try { data = JSON.parse(reply.content) } catch(e) { data = reply.content.toString('utf8') };
		self.emitReply( cid , replyType , data );
	});
	
	self._awaitingReply = {} ;
	
	self._replyEmitter = new events.EventEmitter();
	// we're using this eventEmitter to subscribe to promise-resolving states, 
	// so we'll quickly initialize more than 10 listeners under load. Should
	// this be configurable? I'm not so sure...
	self._replyEmitter.setMaxListeners(0);
	
	
};

Rpc.prototype.ready = function(){
	
	var self = this ;
	
	return self._replyQueue.ready().then(function(){
		return self._exchange.ready().then(function(){
			return self._consumerPromise.then(function(){
				return when({ replyQueue : self._replyQueue , exchange : self._exchange });
			})
		})
	})
	
};

Rpc.prototype.call = function ( routingKey , args , options ) {
	
	var self = this ;
	var cid = uuid.v4(); // more unique b/ not timestamp based
	
	return when.promise(function(resolve,reject,notify){
		
		var replyQueue = self._replyQueue.name;
		var exchange = self._exchange;
		var data = ( typeof args !== undefined ) ? args : null ;
		var message = JSON.stringify(data);
		var opts = _.defaults( options || {} , { 
			mandatory : true , 
			contentType : 'application/json' ,
			correlationId : cid ,
			replyTo : replyQueue ,
			timestamp : Date.now() ,
			type : 'rpc',
			awaitReply : true
		});
		
		var publishPromise = exchange.publish( routingKey , message , opts );

		
		if (opts.awaitReply){
			self.onReply( cid , { 'resolve' : resolve , 'reject' : reject , 'notify' : notify });
			publishPromise.then(null,reject); // reject this call if publish fails 
		} else {
			publishPromise.then(resolve,reject); // resolve or reject this call immediately if a reply is not expected
		}
		
	})
	
};


Rpc.prototype.emitReply = function (cid, replyType, data) {
	var self = this ;
	self._replyEmitter.emit(cid,replyType,data);
};

Rpc.prototype.onReply = function ( cid , resolvers ) {
	
	var self = this ;
	
	self._replyEmitter.on(cid,function listener (replyType,data){
		if ( 'resolve' == replyType || 'reject' == replyType ) {
			self._replyEmitter.removeListener(cid,listener)
		}
		resolvers[replyType](data);
	})
	
}


/*
Rpc.prototype.onReply = function ( cid , resolvers ) {
	
	var self = this ;
	self._awaitingReply[ cid ] = resolvers ;
	
}

Rpc.prototype.emitReply = function ( cid , replyType , data ) {
	
	var self = this ;
	var resolvers = self._awaitingReply[cid] ;
	
	if ( resolvers ) {
		
		if ( 'resolve' == replyType || 'reject' == replyType ) {
			
			delete self._awaitingReply[cid]
		
		}
		
		resolvers[replyType](data);

	}
	
}
*/
Rpc.prototype.expose = function ( q , opts , fn ) {
	
	var self = this ;
	
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
		} else if ( !handler ){
			throw new Error('Supplied Method Handler is not a function.')
		}
		
		queue.ready().then(function( d ){
			
			return queue.bind(self._exchange)
		
		}).then(function(){
			
			return queue.consume(opts,function(message){
				var doAck = !options.noAck ;
				var ack = queue.ack.bind(queue,message) ;
			
				var prom = when.promise(function (resolve, reject, notify) {
					var data = JSON.parse(message.content) ;
					handler( data , resolve , reject , notify ) ;
				});
			
				if ( message.properties.replyTo && message.properties.correlationId ) {
					prom.then(function(data){
						self.sendReply( 'resolve' , message.properties.replyTo , message.properties.correlationId , data );
					}).then(null,function(error){
						var log = format('%s Reject [rpc]:\n%s', queue.name , error.stack||error );
						self._connection.emit('info', log );
						self.sendReply( 'reject', message.properties.replyTo , message.properties.correlationId , error );
					}).then(null,null,function(notification){
						self.sendReply( 'notify' , message.properties.replyTo , message.properties.correlationId , notification );
					});
				}
			
				if ( doAck ) {
					prom.then(ack,ack);
				}
				
				return prom ;
				
			})
			
		}).then(resolve,reject)
		
	})
		
};


Rpc.prototype.sendReply = function ( replyType , replyTo , correlationId , data ) {

	var self = this ;
	var options = { contentType: 'application/json' , correlationId: correlationId , messageId: replyType };
	var message = util.isError(data) ? { 'message' : err.message , 'stack' : err.stack } : data ;

	return self._connection.sendToQueue( replyTo , JSON.stringify(message) , options )
	
}