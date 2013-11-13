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
var serialize = JSON.stringify ;
var parse = JSON.parse ;
var serialize = msgpack.pack ;
var parse = msgpack.unpack ;

util.inherits( RPC , events.EventEmitter ) ;

ready.mixin( RPC.prototype ) ;

module.exports = RPC ;

function RPC ( connection , exchangeOrName , exchangeOpts ) {
	
	var _this = this ;
	
	if ( exchangeOrName instanceof Exchange ) {
		
		_this._exchange = exchangeOrName ;
		
	} else {
		
		_this._exchange = connection.exchange(
			exchangeOrName || _this._exchangeDefaults[0] ,
			exchangeOpts || _this._exchangeDefaults[1]
		);
		
	}
	
	_this._connection = connection ;
	
	// Create our own random queue-name as opposed to having rabbit generate an amq.* name
	// We want to be able to pick up replies sent between reconnections, and we can't rely
	// on a randomly generated name to do that unless we keep re-binding that original name
	
	// make this queue exclusive, such that it only can communicate with this connection
	// and is destroyed when the connection is dropped. This way, any bindings to this queue
	// (such as method-signatures) will be destroyed.
	
	_this._queueName = format('rpc.%s',uuid.v4()) ;
	_this._queue = connection.queue( _this._queueName , { exclusive : true } ) ;
	_this._queue.subscribe( { noAck : true , prefetch : 1 } , _this._routeInternalQueue.bind(_this) ) ;
	_this._initialize() ;
	
	_this._exchange.on('disconnect',function(){
		
		_this.ready(false) ;
		_this.emit('disconnect') ;
		_this._initialize() ;
		
	});

	_this._queue.on('disconnect',function(){
		
		_this.ready(false) ;
		_this.emit('disconnect') ;
		_this._initialize() ;
		
	});

}

RPC.prototype._initialize = function ( routingKey ) {
	
	var _this = this ;
	
	if ( _this._initializing ) return false ;
	
	_this._initializing = true ;
	
	when.all([
		
		_this._queue.ready() ,
		_this._exchange.ready()
		
	]).then(function(){

		return when.map( Object.keys(_this._methods) , function(methodSignature){
			
			return _this._queue.bind( _this._exchange , methodSignature )
			
		});
		
	}).then(function(){
		
		_this._exchange._channel.on('return',function(d){
			
			var cid = d.properties.correlationId ;
			var err = _this._rpcError(d.fields) ;
			
			_this._callPending( cid , { error : err } ) ;
			
		});
		_this._initializing = false ;
		_this.ready(true) ;
	
	}).then( null , function (err) {

		_this._initializing = false ;
		_this.ready(false) ;
		_this.emit('disconnect') ;
		_this.emit( 'error' , err ) ;
	
  });
	
};

RPC.prototype.call = function ( methodSignature , args ) {
	
	var _this = this ;
	var d = serialize(args) ;
	var correlationId = cid = uuid.v4() ;
	var ourQueue = _this._queueName ;
	var deferred = when.defer() ; 
	
	var opts = { 
		mandatory : true , 
		correlationId : correlationId ,
		replyTo : ourQueue ,
		contentType : 'application/json'
	}; 
	
	_this._pending[ correlationId ] = deferred ;
	
	// Set mandatory:true
	//
	// It's important that we do not try to invoke methods 
	// for which there are no active handlers. All of our
	// RPC queues are connection-exclusive, so their bindings 
	// (in this case, the method signatures that they support) 
	// will only be active while connected. We will check for 
	// 'returned' messages and call the cb with an error if 
	// the method is not supported by any currently connected clients

	return _this._exchange.publish( methodSignature , d , opts ).then(function(){
		
		return deferred.promise ;
		
	}); 
	
};

RPC.prototype.expose = function ( methodSignature , fn ) {
	
	var _this = this ;
	var prom = _this.ready().then(function(){
		
		return _this._queue.bind( _this._exchange , methodSignature ) ;
		
	});
	
	prom.then(function(){
		
		_this._methods[ methodSignature ] = fn ;
		
	});
	
	return prom ;
	
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

RPC.prototype._routeInternalQueue = function ( message , ack , nack ) {
	
	var _this = this ;
	var method = _this._methods[ message.fields.routingKey ] ;
	var correlationId = message.properties.correlationId ;
	var routingKey = message.fields.routingKey ;
	
	// this is a reply

	if ( routingKey == _this._queue._name && correlationId ) {
		
		_this._handleReply( message ) ;
	
	} else if ( _this._methods.hasOwnProperty( routingKey ) ) {
	
		_this._handleRequest( message ) ;
	
	}
	
}

RPC.prototype._handleReply = function ( message ) {
	
	var _this = this ;
	var correlationId = message.properties.correlationId ;
	var args = parse(message.content) ;
	var errArg = args[0] ;
	
	_this._callPending( correlationId , args ) ;
	
}

RPC.prototype._handleRequest = function ( message ) {
	
	var _this = this ;
	var method = _this._methods[ message.fields.routingKey ] ;
	var replyTo = message.properties.replyTo ;
	var correlationId = message.properties.correlationId ;
	var args = parse(message.content) ;
	var deferred = when.defer() ;
	var ctx = {
		
		deferred : deferred
		
	}
	
	deferred.promise.then(function(result){
		
		_this._sendReply(replyTo,correlationId,{error:null,result:result})
		
	}).then(null,function(err){
		
		_this._sendReply(replyTo,correlationId,{error:err,result:null})
		
	});
	
	try {
		
		method.apply( ctx , args ) ;	
		
	} catch ( err ) {
		
		deferred.reject(err) ;
		
	}
	
}

RPC.prototype._sendReply = function ( replyTo , correlationId , respData ) {
	
	var _this = this ;
	
	if ( replyTo && correlationId ) {
		
		var opts = { contentType : 'application/json' , correlationId : correlationId } ;

		return _this._connection.sendToQueue( replyTo , serialize(respData) , opts ) ;
		
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
	
	var _this = this ;
	var deferred = _this._pending[ cid ] ;
	
	if ( deferred ) {
		
		delete _this._pending[ cid ]
		
		if ( results.error ) {
			
			deferred.reject(results.error) ;
			
		} else {
			
			deferred.resolve(results.result) ;
			
		}
		
	}
	
};

RPC.prototype._methods = {} ;

RPC.prototype._exchangeDefaults = [ 'rpc.topic' , { type : 'direct' , autoDelete : true , durable : false } ] ;

RPC.prototype._pending = {} ;