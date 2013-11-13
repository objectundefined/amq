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
	
	var self = this ;
	
	self._connection = connection ;
	self._initialize( name , options ) ;
	self._connection.on('disconnect',function(){
		
		self.ready(false) ;
		self.emit('disconnect') ;
		self._initialize( name , options ) ;
		
	});

};

Queue.prototype._initialize = function ( name , options ) {
	
	var self = this ;
	var conf = options && options.confirm || null ;
	var opts = options && _.omit( options , 'type' , 'confirm' ) || {} ;
	
	
	if ( self._initializing ) return false ;
	
	self._initializing = true ;

	self._connection.ready(function(){
		
		var conn = self._connection.conn ;

		conn.createChannel().then(function(channel){
			
			self._channel = channel ;
			
			var ok = channel.assertQueue( name , opts ) ;
			
			ok.then(function(resp){
				
				// to create a transient queue and have rabbitmq generate a name, pass in [name='']
				// name will not be avail for transient queues until ready
				self._name = resp.queue ;
				
			})
			
			return ok ;
			
		}).then( function () {
			
			self.ready(true) ;
			self._initializing = false ;
			
		}).then( null , function ( err ) {
			
			self.ready(false) ;
			self.emit('error',err) ;
			self.emit('disconnect') ;
			self._initializing = false ;
			
		});

	});
	
}

Queue.prototype.publish = function ( message , options , callback ) {
	
	var self = this ;
	var m = Buffer.isBuffer( message ) ? message : new Buffer( message ) ;
	var def = when.defer();
	
	self.ready().then(function(){ 
		
		var ok = self._channel.sendToQueue( self._name , m , options , callback ) ;
		
		if ( ok ) return def.resolve(true) ;
		
		self._channel.once('drain',function(){ def.resolve(true) })
		self._channel.once('error',function(err){ def.reject(err) })
		
	});
	
	return def.promise ;
	
};

Queue.prototype.subscribe = function () {
	
	var self = this ;

	if ( ! self._subscribed && ! self._subscribing ) {
		
		var args = _.toArray(arguments) ;
		var listener = args.pop() ;
		var opts = args.pop() || {} ;
		var prefetch = opts.prefetch || 1 ;
		var options = _.omit(opts,'prefetch') ;
		var doSubscribe = function(){
		
			var channel = self._channel ;
			var listenerShim = function ( data ){
				var ack = channel.ack.bind( channel , data ) ;
				var nack = channel.nack.bind( channel , data ) ;
				listener( data , ack , nack )
			}
		
			var prom = channel.prefetch( prefetch ).then(function(){ 
				return channel.consume( self._name , listenerShim , options ) ;
			});
		
			prom.then(function(d){
				self._consumerTag = d.consumerTag ;
				self._subscribed = true ;
				self._subscribing = false ;
				// if succsessful, resubscribe on reconnect
				self.once('disconnect',function(){
					delete self._consumerTag ;
					if ( self._subscribed ) self.ready( doSubscribe ) ;
				}) ;
			
			}).then(null,function(err){
				
				self._subscribing = false ;
				
			})
		
			return prom ;
		
		};
		
		self._subscribing = true ;
		
		return self.ready().then( doSubscribe );
		
	} else {
		
		var def = when.defer() ;
		def.reject(new Error('Already subscribed')) ;
		return def.promise ;
		
	}
	
};

Queue.prototype.unsubscribe = function () {
	
	var  self = this ;
	
	if ( self._subscribed && self._consumerTag ) {	

		var prom = self._channel.cancel(self._consumerTag) ;
		prom.then(function(){
			self._subscribed = false ;
			delete self._consumerTag ;
		});
		return prom ;

	} else {
		
		var def = when.defer() ;
		def.reject(new Error('Not subscribed')) ;
		return def.promise ;
		
	}
	
};

Queue.prototype.bind = function ( exchange , routingKey , args ) {
	
	var self = this ;
	var exg = _.isString(exchange) ? exchange : exchange._name || '' ; 
	var rk = routingKey || self._name ;
	
	return self.ready().then(function(){ 
		return self._channel.bindQueue( self._name , exg , rk , args  ) 
	})
	
};

Queue.prototype.unbind = function ( exchange , routingKey , args ) {
	
	var self = this ;
	var exg = _.isString(exchange) ? exchange : exchange._name || '' ; 
	
	return self.ready().then(function(){ 
		return self._channel.unbindQueue( self._name , exg , routingKey , args  ) 
	});

};
