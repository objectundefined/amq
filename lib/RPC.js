var events = require('events') ;
var util = require('util') ;
var ready = require('ready') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var format = require('util').format ;
var uuid = require('node-uuid') ;
var Exchange = require('./Exchange') ;

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
	
	_this._queue = connection.queue( format('rpc.%s',uuid.v4()) , { exclusive : true } ) ;
	_this._queue.subscribe( _this._routeInternalQueue.bind(_this) ) ;
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
			
			if ( cid ) _this.emit( format('reply:%s',cid) , err )
			
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

RPC.prototype.call = function ( /* methodSignature , data , moreData , callback */ ) {
	
	var _this = this ;
	var args = _.toArray( arguments ) ;
	var methodSignature = args.shift() ;
	var callback = _.isFunction( _.last( args ) ) ? args.pop() : null ;
	var needReply = !!callback ;
	
	return _this.ready().then(function(){ 
		
		// our transient queue name is not available
		// until the queue is ready and rabbit has assigned one.

		var ourQueue = _this._queue._name ;
		var d = JSON.stringify(args) ;
		var correlationId = uuid.v4() ;
		
		if ( callback ) _this.once( format('reply:%s',correlationId) , callback ) ; 
		
		// Set mandatory:true
		//
		// It's important that we do not try to invoke methods 
		// for which there are no active handlers. All of our
		// RPC queues are connection-exclusive, so their bindings 
		// (in this case, the method signatures that they support) 
		// will only be active while connected. We will check for 
		// 'returned' messages and call the cb with an error if 
		// the method is not supported by any currently connected clients
	
		var opts = { 
			mandatory : true , 
			correlationId : needReply ? correlationId : null ,
			replyTo : needReply ? ourQueue : null ,
			contentType : 'application/json'
		}; 
		
		return _this._exchange.publish( methodSignature , d , opts , function( err ){
			
			if ( err ) _this.emit( format('reply:%s',correlationId ) , err ) ; 
			
		}) 
		
	});	
	
};

RPC.prototype.expose = function ( methodSignature , fn ) {
	
	var _this = this ;
	var prom = _this.ready().then(function(){
		
		return _this._queue.bind( _this._exchange , methodSignature ) ;
		
	});
	
	prom.then(function(){
		
		_this._methods[ methodSignature ] = fn.bind(_this) ;
		
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
	var args = JSON.parse( message.content ) ;
	var correlationId = message.properties.correlationId ;
	var routingKey = message.fields.routingKey ;

	// this is a reply

	if ( routingKey == _this._queue._name && correlationId ) {
		
		return _this._handleReply( message , ack ) ;
	
	} else if ( _this._methods.hasOwnProperty( routingKey ) ) {
	
		return _this._handleRequest( message , ack ) ;
	
	} else {
	
		return nack() ;
	
	}
}

RPC.prototype._handleReply = function ( message , ack ) {
	
	var _this = this ;
	var correlationId = message.properties.correlationId ;
	var args = JSON.parse(message.content) ;
	var errArg = args[0] ;
	
	args[0] = deserializeError( errArg ) ;
	
	_this.emit.apply( _this , [ format('reply:%s', correlationId ) ].concat(args) ) ;
	
	return ack() ;
	
}

RPC.prototype._handleRequest = function ( message , ack ) {
	
	var _this = this ;
	var method = _this._methods[ message.fields.routingKey ] ;
	var args = JSON.parse(message.content) ;
	var replyTo = message.properties.replyTo ;
	var correlationId = message.properties.correlationId ;
	var done = function () { ack() } ;
	
	try {
		
		method.apply( _this , args.concat(function(){
			
			_this._sendReply( replyTo , correlationId , _.toArray(arguments) ).then( done , done );
			
		})) ;	
		
	} catch ( err ) {
		
		_this._sendReply( replyTo , correlationId , [ err ] ).then( done , done ) ;
		
	}
	
}

RPC.prototype._sendReply = function ( replyTo , correlationId , respData ) {
	
	var _this = this ;
	
	if ( replyTo && correlationId ) {
		
		var opts = { contentType : 'application/json' , correlationId : correlationId } ;
		var errArg = respData[0] ;
	
		respData[ 0 ] = serializeError( errArg ) ;
	
		return _this._connection.sendToQueue( replyTo , new Buffer(JSON.stringify(respData)) , opts ) ;
		
	} else {
		
		var def = when.defer() ;
		
		def.resolve() ;
		
		return def.promise ;
		
	}
	
}

RPC.prototype._methods = {} ;

RPC.prototype._exchangeDefaults = [ 'rpc.topic' , { confirm : true , type : 'direct' , autoDelete : true , durable : false } ] ;


function serializeError ( err ) {
	
	if ( err && util.isError( err ) ) {
		
    var alt = {rpc_error:true};

    Object.getOwnPropertyNames(err).forEach(function (key) {
        alt[key] = err[key];
    }, err);

    return alt;
		
	}
	
	return err ;
	
}

function deserializeError ( err ) {
	
	if ( err && err.rpc_error ) {
		
	  var error = new Error();

	  Object.getOwnPropertyNames(err).forEach(function (key) {
	      if ( key != 'rpc_error' ) err[key] = err[key];
	  }, err);
		
		return error ;
		
	}

  return err;
	
}
