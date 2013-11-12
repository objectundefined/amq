var events = require('events') ;
var util = require('util') ;
var ready = require('ready') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var format = require('util').format ;

util.inherits( BoundQueue , events.EventEmitter ) ;

ready.mixin( BoundQueue.prototype ) ;

module.exports = BoundQueue ;

function BoundQueue ( exchange , queue , routingKey ) {
	
	var _this = this ;
	
	_this._exchange = exchange ;
	_this._queue = queue ;
	_this._initialize( routingKey ) ;
	
	_this._exchange.on('disconnect',function(){
		
		_this.ready(false) ;
		_this.emit('disconnect') ;
		_this._initialize( routingKey ) ;
		
	});

	_this._queue.on('disconnect',function(){
		
		_this.ready(false) ;
		_this.emit('disconnect') ;
		_this._initialize( routingKey ) ;
		
	});

	
}

BoundQueue.prototype._initialize = function ( routingKey ) {
	
	var _this = this ;
	
	if ( _this._initializing ) return false ;
	
	_this._initializing = true ;
	
	_this._queue.ready().then(function(){
		
		// name will not be avail for transient queues until ready
		_this._routingKey = routingKey  || _this._queue._name ; 
		return _this._queue.bind(_this._exchange._name , _this._routingKey)
		
	}).then(function(){
		
		return _this._exchange.ready() ;
		
	}).then(function(){

		_this._initializing = false ;
		_this.ready(true) ;
	
	}).then( null , function (err) {

		_this._initializing = false ;
		_this.ready(false) ;
		_this.emit('disconnect') ;
		_this.emit( 'error' , err ) ;
	
  });
	
};

BoundQueue.prototype.publish = function ( message, options, callback ) {
	
	var _this = this ;

	return _this.ready().then(function(){ return _this._exchange.publish( _this._routingKey , message , options , callback ) })	
	
};

BoundQueue.prototype.subscribe = function () {
	
	var _this = this ;
	var args = _.toArray(arguments);
	
	return _this.ready().then(function(){ return _this._queue.subscribe.apply( _this._queue , args ) }) ;
		
};