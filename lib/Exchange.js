var events = require('events') ;
var util = require('util') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var format = require('util').format ;
var BoundQueue = require('./BoundQueue') ;
var recant = require('recant') ;

util.inherits( Exchange , events.EventEmitter ) ;

module.exports = Exchange ;

function Exchange ( connection , name , options ) {
	
	var self = this ;
	
	self._connection = connection ;
	self._name = name ;
	self._type = options && options.type || 'topic' ;
	self._confirm = options && options.confirm || false ;
	self._opts = options && _.omit( options , 'type' , 'confirm' ) || {} ;
	self._channelPromise = self._openChannel() ;
	self._exchangePromise = self._openExchange() ;
	
};

Exchange.prototype.ready = function(){
	
	var self = this ;
	
	return self._connection.ready().then(function(){
		return self._channelPromise.then(function(){
			return self._exchangePromise;
		})
	})
	
};

Exchange.prototype._openChannel = function () {
	var self = this;
	var conf = self._confirm ; 
	var opts = self._opts ;
	var type = self._type ;
	var name = self._name ;
	
	return recant.promise(function(resolve,reject,notify,reset){
		self._connection.ready().then(function(conn){
			var createChannel = conf ? conn.createConfirmChannel.bind(conn) : conn.createChannel.bind(conn) ;
			createChannel().then(function(channel){
				resolve(channel);
				channel.once('error',reset);
				channel.once('close',reset);
			}).then(null,reject);
		}).then(null,reject);
	});
	
};

Exchange.prototype._openExchange = function () {
	
	var self = this ;
	var conf = self._confirm ; 
	var opts = self._opts ;
	var type = self._type ;
	var name = self._name ;
	var isAmqExchange = !name || name.indexOf('amq.') == 0 ;
	
	return recant.promise(function(resolve,reject,notify,reset){
		self._channelPromise.then(function(channel){
			if ( !isAmqExchange ) return channel.assertExchange( name , type , opts )
		}).then( resolve , reject );
	})

}

Exchange.prototype.boundQueue = function ( q , routingKey ) {
	
	var self = this ;
	
	return new BoundQueue( self , q , routingKey ) ;
	
};

Exchange.prototype.publish = function ( routingKey , message , options , callback ) {
	
	var self = this ;
	var m = Buffer.isBuffer( message ) ? message : new Buffer( message ) ;
	
	return self.ready().then(function(){
		return self._channelPromise ;
	}).then(function(channel){
		var deferred = when.defer();
		var ok = channel.publish( self._name , routingKey , m , options , callback ) ;
		if ( ok ) {
			deferred.resolve(true) ;
		} else {
			channel.once('drain',function(){ deferred.resolve(true) })
			channel.once('error', deferred.reject.bind(deferred) )	
		}
		return deferred.promise;
	});
	
};


Exchange.prototype.bind = function ( destExchange , routingKey , args ) {
	
	var self = this ;
	var exg = _.isString(destExchange) ? destExchange : destExchange._name || '' ; 
	
	return self.ready().then(function(){ 
		return self._channelPromise ;
	}).then(function(channel){
		return channel.bindExchange(  exg , self._name , routingKey , args  )
	});
	
};

Exchange.prototype.unbind = function ( destExchange , routingKey , args ) {
	
	var self = this ;
	var exg = _.isString(destExchange) ? destExchange : destExchange._name || '' ; 
	
	return self.ready().then(function(){ 
		return self._channelPromise ;
	}).then(function(channel){
		return channel.unbindExchange(  exg , self._name , routingKey , args  )
	});
	
};