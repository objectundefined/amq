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
	_this._name = name ;
	_this._prefetchCount = options && options.prefetchCount || null ;
	_this._opts = options && _.omit( options , 'type' , 'confirm' , 'prefetchCount' ) || {} ;
	_this._initialize() ;
	_this._connection.on('disconnect',function(){
		
		_this.ready(false) ;
		_this.emit('disconnect') ;
		_this._initialize() ;
		
	});

};

Queue.prototype._initialize = function () {
	
	var _this = this ;
	var conf = _this._confirm ; 
	var opts = _this._opts ;
	var type = _this._type ;
	var name = _this._name ;
	
	
	if ( _this._initializing ) return false ;
	
	_this._initializing = true ;

	_this._connection.ready(function(){
		
		var conn = _this._connection.conn ;
		var createChannel = conf ? conn.createConfirmChannel() : conn.createChannel() ;

		createChannel.then(function(channel){
			
			_this._channel = channel ;
			
			return when.all([
				
				channel.assertQueue( name , type , opts ) ,
				channel.prefetch( _this._prefetchCount )
				
			]);
			
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
	
	return _this.ready().then(function(){ 
		return 	_this._channel.sendToQueue( _this._name , m , options , callback ) ;
	})
		
};


Queue.prototype.subscribe = function () {
	
	var _this = this ;

	if ( ! _this._subscribed ) {
		
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
				// if succsessful, resubscribe on reconnect
				_this.once('disconnect',function(){
					delete _this._consumerTag ;
					if ( _this._subscribed ) _this.ready( doSubscribe ) ;
				}) ;
			
			})
		
			return prom ;
		
		};
		
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
	
	return _this.ready().then(function(){ 
		return _this._channel.bindQueue( _this._name , exg , routingKey , args  ) 
	})
	
};


Queue.prototype.unbind = function ( exchange , routingKey , args ) {
	
	var _this = this ;
	var exg = _.isString(exchange) ? exchange : exchange._name || '' ; 
	
	return _this.ready().then(function(){ 
		return _this._channel.unbindQueue( _this._name , exg , routingKey , args  ) 
	});

};
