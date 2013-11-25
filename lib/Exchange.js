var events = require('events') ;
var util = require('util') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var format = require('util').format ;
var recant = require('recant') ;

util.inherits( Exchange , events.EventEmitter ) ;

module.exports = Exchange ;

function Exchange ( connection , name , options ) {
	
	var self = this ;

	Object.defineProperty(self,'_connection', { value: connection });
	Object.defineProperty(self,'_type', { value: (options && options.type || 'topic') });
	Object.defineProperty(self,'_confirm',{ value: (options && options.confirm || false) })
	Object.defineProperty(self,'_opts',{ value: (options && _.omit( options , 'type' , 'confirm' ) || {}) })
	self.name = name ;	
	self._channelPromise = self._openChannel() ;
	self._exchangePromise = self._openExchange() ;
	
};

Exchange.prototype.ready = function(){
	
	var self = this ;
	
	return self._connection.ready().then(function(conn){
		return self._channelPromise.then(function(ch){
			return self._exchangePromise.then(function(exg){
				return when({ connection : conn , channel : ch , exchange : exg })
			})
		})
	})
	
};

Exchange.prototype.open = function () {
	
	var self = this ;
	
	return self._openExchange() ;
	
};

Exchange.prototype._openChannel = function () {
	var self = this;
	var conf = self._confirm ; 
	var opts = self._opts ;
	var type = self._type ;
	var name = self.name ;
	
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
	var name = self.name ;
	var isAmqExchange = !name || name.indexOf('amq.') == 0 ;
	var opts = isAmqExchange ? undefined : self._opts ; // dont pass args if its a default exchange
	var type = isAmqExchange ? undefined : self._type ; // dont pass args if its a default exchange
	
	return recant.promise(function(resolve,reject,notify,reset){
		self._channelPromise.then(function(channel){
			channel.once('error',reset);
			channel.once('close',reset);
			return channel.assertExchange( name , type , opts );
		}).then( resolve , reject );
	})

}

Exchange.prototype.publish = function ( routingKey , message , options , callback ) {
	
	var self = this ;
	var m = Buffer.isBuffer( message ) ? message : new Buffer( message ) ;
	
	return self.ready().then(function(d){
		return when.promise(function(resolve,reject){
			var ok = d.channel.publish( self.name , routingKey , m , options , callback ) ;
			if ( ok ) {
				resolve(true) ;
			} else {
				d.channel.once('drain',function(){ resolve(true) })
				d.channel.once('error', function(err){ reject(err) } )	
			}
		});
	});
};


Exchange.prototype.bind = function ( destExchange , routingKey , args ) {
	
	var self = this ;
	var exg = _.isString(destExchange) ? destExchange : destExchange._name || '' ; 
	
	return self.ready().then(function(d){
		return d.channel.bindExchange(  exg , self.name , routingKey , args  )
	});
	
};

Exchange.prototype.unbind = function ( destExchange , routingKey , args ) {
	
	var self = this ;
	var exg = _.isString(destExchange) ? destExchange : destExchange._name || '' ; 
	
	return self.ready().then(function(d){
		return d.channel.unbindExchange(  exg , self.name , routingKey , args  )
	});
	
};