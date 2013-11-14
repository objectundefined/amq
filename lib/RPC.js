var events = require('events') ;
var msgpack = require('msgpack') ;
var util = require('util') ;
var ready = require('ready') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var format = require('util').format ;
var uuid = require('node-uuid') ;
var Exchange = require('./Exchange') ;
var Queue = require('./Queue') ;
var serialize = JSON.stringify ;
var parse = JSON.parse ;
var serialize = msgpack.pack ;
var parse = msgpack.unpack ;

util.inherits( RPC , events.EventEmitter ) ;

ready.mixin( RPC.prototype ) ;

module.exports = RPC ;

function RPC ( connection , exchangeOrName , exchangeOpts ) {
	
	var self = this ;
	
	if ( exchangeOrName instanceof Exchange ) {
		
		self._exchange = exchangeOrName ;
		
	} else {
		
		self._exchange = connection.exchange(
			exchangeOrName || self._exchangeDefaults[0] ,
			exchangeOpts || self._exchangeDefaults[1]
		);
		
	}
	
	self._connection = connection ;
	
	// Create our own random queue-name as opposed to having rabbit generate an amq.* name
	// We want to be able to pick up replies sent between reconnections, and we can't rely
	// on a randomly generated name to do that unless we keep re-binding that original name
	
	// make this queue exclusive, such that it only can communicate with this connection
	// and is destroyed when the connection is dropped. This way, any bindings to this queue
	// (such as method-signatures) will be destroyed.
	
	self._queueName = format('rpc.%s',uuid.v4()) ;
	self._queue = connection.queue( self._queueName , { exclusive : true } ) ;
	self._queue.subscribe( { noAck : true , prefetch : 0 } , self._handleReply.bind(self) ) ;
	self._initialize() ;
	
	self._exchange.on('disconnect',function(){
		
		self.ready(false) ;
		self.emit('disconnect') ;
		self._initialize() ;
		
	});

	self._queue.on('disconnect',function(){
		
		self.ready(false) ;
		self.emit('disconnect') ;
		self._initialize() ;
		
	});

}

RPC.prototype._initialize = function ( routingKey ) {
	
	var self = this ;
	
	if ( self._initializing ) return false ;
	
	self._initializing = true ;
	
	when.all([
		
		self._queue.ready() ,
		self._exchange.ready()
		
	]).then(function(){
		
		self._exchange._channel.on('return',function(d){
			
			var cid = d.properties.correlationId ;
			var err = self._rpcError(d.fields) ;
			
			self._callPending( cid , { error : err } ) ;
			
		});
		self._initializing = false ;
		self.ready(true) ;
	
	}).then( null , function (err) {

		self._initializing = false ;
		self.ready(false) ;
		self.emit('disconnect') ;
		self.emit( 'error' , err ) ;
	
  });
	
};

RPC.prototype.call = function ( methodSignature , args , options ) {
	
	var self = this ;
	var d = serialize(args) ;
	var cid = uuid.v4() ;
	var q = self._queueName ;
	var deferred = when.defer() ;
	var opts = 	_.defaults( options || {}, { awaitReply : true }) ;
	var pubOpts = _.omit(opts,'reply') ;
	var publish ; 
	
	pubOpts.contentType = 'application/json' ;
	
	if ( opts.awaitReply ) {
		self._pending[ cid ] = deferred ;
		_.extend( pubOpts , { correlationId : cid , replyTo : q  }) ;
	} else {
		deferred.resolve(true) ;
	}
	
	if ( methodSignature instanceof Queue ) {
		publish = methodSignature.publish( d , pubOpts ) ;
	} else {
		// Set mandatory:true
		// It's important that we do not try to invoke methods 
		// for which there are no active handlers.
		pubOpts.mandatory = true ;
		publish = self._exchange.publish( methodSignature , d , pubOpts ) ;
	}
	
	return publish.then(function(){
		return deferred.promise ;
	})
	
};

RPC.prototype.expose = function ( methodSignature , fn ) {
	
	var self = this ;
	var conn = self._connection ;
	var queue = ( methodSignature instanceof Queue )
		? methodSignature 
		: conn.queue( methodSignature , { autoDelete : true });
	
	return queue.ready().then(function(){
		
		return queue.bind( self._exchange ) ;
		
	}).then(function(){
		
		return queue.subscribe({ prefetch : 1000 },function( message , ack , nack ){
			
			var replyTo = message.properties.replyTo ;
			var correlationId = message.properties.correlationId ;
			var args = parse(message.content) ;
			var deferred = when.defer() ;
			var ctx = { deferred : deferred } ;
			
      deferred.promise.then(function(result){
      
      	self._sendReply(replyTo,correlationId,{error:null,result:result}).then( ack , ack )
        
      }).then(null,function(err){
        
				self._sendReply(replyTo,correlationId,{error:err,result:null}).then( ack , ack )
        
      });
        
      try {
        
        fn.apply( ctx , args ) ;        
        
      } catch ( err ) {
        
				deferred.reject(err) ;
        
      }
			
		})
		
	})
	
};

RPC.prototype._rpcError = function ( fields ) {
	
	var err = new Error() ;
	err.name = "RPC Error" ;
	err.code = fields.replyCode ;
	err.message = fields.replyText ;
	
	
	switch ( err.code ) {
		
		// add more...
		
		case 312 : 
			err.name = 'Route Error' ;
			err.message = 'Procedure not exposed by any connected clients.'
			break;
			
	}
	
	return err ;
	
};

RPC.prototype._handleReply = function ( message ) {
	
	var self = this ;
	var correlationId = message.properties.correlationId ;
	var args = parse(message.content) ;
	var errArg = args[0] ;
	
	self._callPending( correlationId , args ) ;
	
}

RPC.prototype._sendReply = function ( replyTo , correlationId , respData ) {
	
	var self = this ;
	
	if ( replyTo && correlationId ) {
		
		var opts = { contentType : 'application/json' , correlationId : correlationId } ;

		return self._connection.sendToQueue( replyTo , serialize(respData) , opts ) ;
		
	} else {
		
		var def = when.defer() ;
		
		def.resolve() ;
		
		return def.promise ;
		
	}
	
}

RPC.prototype._serializeError = function ( item ) {
	
	if ( util.isError( item ) ) {
		
		return { message : item.toString() , stack : item.stack || undefined , code : item.code || undefined }
		
	} else {
		
		return item ;
		
	}
	
};

RPC.prototype._callPending = function ( cid , results ) {
	
	var self = this ;
	var deferred = self._pending[ cid ] ;
	
	if ( deferred ) {
		
		delete self._pending[ cid ]
		
		if ( results.error ) {
			
			deferred.reject(results.error) ;
			
		} else {
			
			deferred.resolve(results.result) ;
			
		}
		
	}
	
};

RPC.prototype._queues = {} ;

RPC.prototype._exchangeDefaults = [ 'rpc.direct' , { type : 'direct' , autoDelete : true , durable : false } ] ;

RPC.prototype._pending = {} ;