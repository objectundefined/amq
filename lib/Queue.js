var events = require('events') ;
var util = require('util') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var format = require('util').format ;
var recant = require('recant') ;
var uuid = require('node-uuid') ;

util.inherits( Queue , events.EventEmitter ) ;

module.exports = Queue ;


function Queue ( connection , name , options ) {
	
	var self = this ;
	
	Object.defineProperty(self,'_connection', { value: connection });
	Object.defineProperty(self,'_confirm',{ value: (options && options.confirm || false) })
	Object.defineProperty(self,'_opts',{ value: (options && _.omit( options , 'confirm' ) || {}) })

	self.name = name ;
	self.prefetch = (options && options.prefetch) || null ;
	self._channelPromise = self._openChannel( self._confirm ) ;
	self._queuePromise = self._openQueue() ;
	
};

Queue.prototype.ready = function(){
	
	var self = this ;
	
	return self._channelPromise.then(function(ch){
		return self._queuePromise.then(function(q){
			return when({ queue : q , channel : ch })
		})
	})
	
};

Queue.prototype.open = function () {
	
	var self = this ;
	
	self._queuePromise = self._openQueue() ;
	return self.ready().then(function(){
		return(when(self));
	}) ;
	
}

Queue.prototype._openChannel = function (conf) {
	
	var self = this;
	var conf = conf ; 
	
	return recant.promise(function(resolve,reject,notify,reset){
		self._connection.ready().then(function(conn){
			var createChannel = conf ? conn.createConfirmChannel.bind(conn) : conn.createChannel.bind(conn) ;
			createChannel().then(function(channel){
				var doReset = function () {
					channel.removeListener('error',doReset);
					channel.removeListener('close',doReset);
					reset();
				};
				channel.once('error',doReset);
				channel.once('close',doReset);
				resolve(channel);
			}).then(null,reject);
		}).then(null,reject);
	});
	
};

Queue.prototype._openQueue = function ( ) {
	
	var self = this ;
	var conf = self._confirm ; 
	var opts = self._opts ;
	var type = self._type ;
	var name = self.name ;
	var wasAsserted = false ;
	var wasReset = false;
	
	return recant.promise(function(resolve,reject,notify,reset){
		self._channelPromise.then(function(channel){
			var chk = wasAsserted ? channel.checkQueue(name) : channel.assertQueue( name , opts ) ;
			wasAsserted = true;
			chk.then(function(q){
				var doReset = function () {
					channel.removeListener('error',doReset);
					channel.removeListener('close',doReset);
					wasReset = true;
					reset();
				};
				channel.once('error',doReset);
				channel.once('close',doReset);
				self.name = q.name = q.queue ;
				resolve(q);
			}).then(null, reject );
		}).then(null, reject );
	});

}

Queue.prototype.publish = function ( message , options , callback ) {
	
	var self = this ;
	var m = Buffer.isBuffer( message ) ? message : new Buffer( message.toString() ) ;
	
	return self.ready().then(function(d){
		var ch = d.channel;
		return when.promise(function(resolve,reject){
			var ok = ch.publish( '' , d.queue.name , m , options , callback ) ;
			if ( ok ) {
				resolve(true) ;
			} else {
				var doResolve = function () {
					resolve(true);
					ch.removeListener('error',doReject);
				};
				var doReject = function (err) {
					reject(err);
					ch.removeListener('drain',doResolve);
				};
				ch.once('drain', doResolve);
				ch.once('error',doReject);
			};
		});
	});
	
};


Queue.prototype.bind = function ( destExchange , patt , args ) {
	
	var self = this ;
	
	return self.ready().then(function(d){
		var exg = _.isString(destExchange) ? destExchange : destExchange.name || '' ; 
		var routingKey = patt || d.queue.name ;
		
		return d.channel.bindQueue(  d.queue.name , exg , routingKey , args  );
	});
	
};

Queue.prototype.unbind = function ( destExchange , patt , args ) {
	
	var self = this ;

	return self.ready().then(function(d){
		var exg = _.isString(destExchange) ? destExchange : destExchange.name || '' ; 
		var routingKey = patt || d.queue.name ;

		return d.channel.unbindQueue(  d.queue.name , exg , routingKey , args  );
	});
	
};

Queue.prototype.consume = function (o,fn) {
	var self = this ;
	var cb , options;
	if ( typeof o == 'function' ) {
		cb = o ;
		options = {} ;
	} else {
		cb = fn ;
		options = o ;
	}
	var opts = _.defaults(options,{ consumerTag : uuid() });
	var resetOnReconnect = true ;
	return recant.promise(function(resolve,reject,notify,reset){
		if ( typeof cb !== 'function' ) return reject( new Error('Queue#consume: cb is not a function') );
		self.ready().then(function(d){
			var unbind = function (){
				d.channel.removeListener('error',doReset);
				d.channel.removeListener('close',doReset);
			}
			var doReset = function (){
				unbind();
				reset();
			}
			d.channel.once('error',doReset);
			d.channel.once('close',doReset);
			self.once('cancel:'+opts.consumerTag, unbind );
			return d.channel.prefetch(self.prefetch).then(function(){
				return d.channel.consume( d.queue.name , cb , opts );
			})
		}).then(resolve,reject);
	});
};

Queue.prototype.cancel = function ( consumerTag ) {
	var self = this ;
	self.emit('cancel:'+consumerTag);
	return self._channelPromise.then(function ( channel ) {
		return channel.cancel( consumerTag )
	})
};

Queue.prototype.get = function ( opts ) {
	var self = this ;
	return self.ready().then(function(d){
		return d.channel.get(  d.queue.name , opts  );
	});
};

Queue.prototype.ack = function ( message , allUpTo ){
	var self = this ;
	return self._channelPromise.then(function (channel) {
		return channel.ack( message , allUpTo )
	})
}

Queue.prototype.ackAll = function (){
	var self = this ;
	return self._channelPromise.then(function (channel) {
		return channel.ackAll()
	})
}

Queue.prototype.nackAll = function (requeue){
	var self = this ;
	return self._channelPromise.then(function (channel) {
		return channel.nackAll(requeue)
	})
}


Queue.prototype.nack = function ( message , allUpTo , requeue ){
	var self = this ;
	return self._channelPromise.then(function ( channel ) {
		return channel.nack( message , allUpTo , requeue )
	})
}

Queue.prototype.recover = function (){
	var self = this ;
	return self._channelPromise.then(function ( channel ) {
		return channel.recover()
	})
}

Queue.prototype.destroy = function ( opts ) {
	var self = this ;
	return self.ready().then(function ( d ) {
		return d.channel.deleteQueue( d.queue.name , opts ).then(function(res){
			self._queuePromise = self._openQueue();
			return when(res);
		})
	})
};

Queue.prototype.purge = function ( opts ) {
	var self = this ;
	return self.ready().then(function ( d ) {
		return d.channel.purgeQueue( d.queue.name );
	})
};

Queue.prototype.check = function () {
	var self = this ;
	return self._channelPromise.then(function ( channel ) {
		return channel.checkQueue( self.name )
	})
}