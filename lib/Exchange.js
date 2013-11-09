var events = require('events') ;
var util = require('util') ;
var ready = require('ready') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var format = require('util').format ;
var BoundQueue = require('./BoundQueue') ;

util.inherits( Exchange , events.EventEmitter ) ;

ready.mixin( Exchange.prototype ) ;

module.exports = Exchange ;

function Exchange ( connection , name , options ) {
	
	var _this = this ;
	
	_this._connection = connection ;
	_this._name = name ;
	_this._type = options && options.type || 'topic' ;
	_this._confirm = options && options.confirm || false ;
	_this._opts = options && _.omit( options , 'type' , 'confirm' ) || {} ;
	_this._initialize() ;
	
	_this._connection.on('disconnect',function(){
		
		_this.ready(false) ;
		_this.emit('disconnect') ;
		_this._initialize() ;
		
	});

};

Exchange.prototype._initialize = function () {
	
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
			
			return channel.assertExchange( name , type , opts )
						
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

Exchange.prototype.boundQueue = function ( q , routingKey ) {
	
	var _this = this ;
	
	return new BoundQueue( _this , q , routingKey ) ;
	
} ;

Exchange.prototype.publish = function ( routingKey , message , options , callback ) {
	
	var _this = this ;
	var m = new Buffer( message ) ;
	
	return _this.ready().then(function(){ return _this._channel.publish( _this._name , routingKey , m , options , callback ) }) ;
	
};


Exchange.prototype.bind = function ( destExchange , routingKey , args ) {
	
	var _this = this ;
	var exg = _.isString(destExchange) ? destExchange : destExchange._name || '' ; 
	
	return _this.ready().then(function(){ 
		return _this._channel.bindExchange(  exg , _this._name , routingKey , args  ) 
	})
	
};

Exchange.prototype.unbind = function ( destExchange , routingKey , args ) {
	
	var _this = this ;
	var exg = _.isString(destExchange) ? destExchange : destExchange._name || '' ; 
	
	return _this.ready().then(function(){ 
		return _this._channel.unbindExchange(  exg , _this._name , routingKey , args  ) 
	})
	
};