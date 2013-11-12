var events = require('events') ;
var util = require('util') ;
var ready = require('ready') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var format = require('util').format ;
var BoundQueue = require('./BoundQueue') ;

util.inherits( Queue , events.EventEmitter ) ;

ready.mixin( Queue.prototype ) ;

module.exports = Queue ;

function Queue ( connection , name , options ) {
	
	var _this = this ;
	
	_this._connection = connection ;
	_this._initialize( name , options ) ;
	_this._connection.on('disconnect',function(){
		
		_this.ready(false) ;
		_this.emit('disconnect') ;
		_this._initialize( name , options ) ;
		
	});

};

Queue.prototype._initialize = function ( name , options ) {
	
	var _this = this ;
	var conf = options && options.confirm || null ;
	var opts = options && _.omit( options , 'type' , 'confirm' , 'prefetchCount' ) || {} ;
	
	
	if ( _this._initializing ) return false ;
	
	_this._initializing = true ;

	_this._connection.ready(function(){
		
		var conn = _this._connection.conn ;

		conn.createChannel().then(function(channel){
			
			_this._channel = channel ;
			
			var ok = channel.assertQueue( name , opts ) ;
			
			ok.then(function(resp){
				
				// to create a transient queue and have rabbitmq generate a name, pass in [name='']
				// name will not be avail for transient queues until ready
				_this._name = resp.queue ;
				
			})
			
			return ok ;
			
		}).then( function () {
			
			_this.ready(true) ;
			_this._initializing = false ;
			
		}).then( null , function ( err ) {
			
			_this.ready(false) ;
			_this.emit('error',err) ;
			_this.emit('disconnect') ;
			_this._initializing = false ;
			
		});

	});
	
}

Queue.prototype.publish = function ( message , options , callback ) {
	
	var _this = this ;
	var m = new Buffer( message ) ;
	var def = when.defer();
	
	_this.ready().then(function(){ 
		
		var ok = _this._channel.sendToQueue( _this._name , m , options , callback ) ;
		
		if ( ok ) return def.resolve(true) ;
		
		_this._channel.once('drain',function(){ def.resolve(true) })
		_this._channel.once('error',function(err){ def.reject(err) })
		
	});
	
	return def.promise ;
	
};

Queue.prototype.subscribe = function () {
	
	var _this = this ;

	if ( ! _this._subscribed && ! _this._subscribing ) {
		
		var args = _.toArray(arguments) ;
		var listener = args.pop() ;
		var opts = args.pop() || {} ;
		var prefetch = opts.prefetch || 1 ;
		var options = _.omit(opts,'prefetch') ;
		var doSubscribe = function(){
		
			var channel = _this._channel ;
			var listenerShim = function ( data ){
				var ack = channel.ack.bind( channel , data ) ;
				var nack = channel.nack.bind( channel , data ) ;
				listener( data , ack , nack )
			}
		
			var prom = channel.prefetch( prefetch ).then(function(){ 
				return channel.consume( _this._name , listenerShim , options ) ;
			});
		
			prom.then(function(d){
				_this._consumerTag = d.consumerTag ;
				_this._subscribed = true ;
				_this._subscribing = false ;
				// if succsessful, resubscribe on reconnect
				_this.once('disconnect',function(){
					delete _this._consumerTag ;
					if ( _this._subscribed ) _this.ready( doSubscribe ) ;
				}) ;
			
			}).then(null,function(err){
				
				_this._subscribing = false ;
				
			})
		
			return prom ;
		
		};
		
		_this._subscribing = true ;
		
		return _this.ready().then( doSubscribe );
		
	} else {
		
		var def = when.defer() ;
		def.reject(new Error('Already subscribed')) ;
		return def.promise ;
		
	}
	
};

Queue.prototype.unsubscribe = function () {
	
	var  _this = this ;
	
	if ( _this._subscribed && _this._consumerTag ) {	

		var prom = _this._channel.cancel(_this._consumerTag) ;
		prom.then(function(){
			_this._subscribed = false ;
			delete _this._consumerTag ;
		});
		return prom ;

	} else {
		
		var def = when.defer() ;
		def.reject(new Error('Not subscribed')) ;
		return def.promise ;
		
	}
	
};

Queue.prototype.bind = function ( exchange , routingKey , args ) {
	
	var _this = this ;
	var exg = _.isString(exchange) ? exchange : exchange._name || '' ; 
	var rk = routingKey || _this._name ;
	
	return _this.ready().then(function(){ 
		return _this._channel.bindQueue( _this._name , exg , rk , args  ) 
	})
	
};

Queue.prototype.unbind = function ( exchange , routingKey , args ) {
	
	var _this = this ;
	var exg = _.isString(exchange) ? exchange : exchange._name || '' ; 
	
	return _this.ready().then(function(){ 
		return _this._channel.unbindQueue( _this._name , exg , routingKey , args  ) 
	});

};
