var events = require('events') ;
var querystring = require('querystring') ;
var util = require('util') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var Re = require('re') ;
var recant = require('recant') ;
var format = require('util').format ;
var Exchange = require('./Exchange') ;
var Queue = require('./Queue') ;
var Rpc = require('./Rpc') ;

util.inherits( Connection , events.EventEmitter ) ;

module.exports = Connection ;

function Connection ( amqpOpts , sockOpts ) {
	
	var self = this ;
	var debug = self._debug = amqpOpts && amqpOpts.debug ;
	
	self._url = self._buildUrl( amqpOpts , sockOpts ) ;
	self._socketOpts = self._buildSocketOpts( sockOpts ) ;
	self._reconnectOpts = self._buildReconnectOpts( sockOpts && sockOpts.reconnect )

	self.on( 'error' , function(error){

		if ( debug ) util.log( format( 'AMQP %s' , error.stack && error.stack || error.toString()  ) )
		self.emit('error',error) ;
		
	});

	self.on( 'info' , function(info){

		if ( debug ) util.log( format( '%s %s' ,self._url, info ) ) ;
		
	});

	self.on( 'close' , function(){
		
		self.conn = undefined ;
		self._openPromise = undefined ;
		self.emit('disconnect') ;
		
		if ( debug ) util.log( 'AMQP Connection Closed' ) ;
		
	});
	
	self._openPromise = self._open() ;
	self._auxChannelPromise = self._openAuxChannel() ;
	
	return self ;
	
};

Connection.prototype.ready = function(){
	var self = this;
	return self._openPromise ;
};

Connection.prototype.open = function() {
	
	var self = this ;
	
	if ( self._closedManually ) {
		delete self._closedManually ;
		self._openPromise = self._open() ;
	}
	
	return self._openPromise ;
	
};

Connection.prototype.close = function () {
	
	var self = this ;
	var prom = when.promise(function(resolve){
		self._closedManually = true ;
		resolve();
	})
	
	if ( this.conn ) {
		return prom.then(function(){
			return this.conn.close() ;
		})
	}
	
	return prom ;
	
};

Connection.prototype._open = function () {
	
	var self = this ;
	var re = new Re(self._reconnectOpts) ;
	var url = self._url ;
	var socketOpts = self._socketOpts ;
	var promise = recant.promise(function(resolve,reject,notify,reset){
		
		re.try(function(retryCount, done){
	
			self.emit('info', format( 'connecting attempt [%s]' , retryCount+1 ) )
			amqplib.connect( url , socketOpts ).then(function(conn) {
				done(null,conn,retryCount+1);
			}).then(null,function(err){
				done(err);
			})

		},function(err, conn, retryCount){
			if ( err ) {
				self.emit('info', format('exhausted recconnection after %s attempts' , retryCount ) ) ;
				reject(err) ;
			} else {
				self.emit('info', format('connected after %s attempts',retryCount) ) ;
				var doReset = function () {
					conn.removeListener('error',doReset);
					conn.removeListener('close',doReset);
					if ( ! self._closedManually ) reset();
				};
				conn.once( 'error' , doReset );
				conn.once('close', doReset );
				resolve( conn ) ;
			}
	
		});
		
	})
	
	return promise ;
	
};

Connection.prototype._openAuxChannel = function () {
	var self = this;
	return recant.promise(function(resolve,reject,notify,reset){
		self._openPromise.then(function(conn){
			conn.createChannel().then(function(channel){
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

Connection.prototype._buildReconnectOpts = function ( opts ) {

	var self = this ;
	var reconnOpts = opts || { retries : 0 } ;
	var retries = !isNaN(reconnOpts.retries) ? reconnOpts.retries : Infinity ;
	var max = !isNaN(reconnOpts.max) ? reconnOpts.max : Infinity ;
	var initial = !isNaN(reconnOpts.initial) ? reconnOpts.initial : 500 ;
	var base = !isNaN(reconnOpts.base) ? reconnOpts.base : 2 ;
	var reOpts = { retries : retries , strategy : { max : max , initial : initial } }
	
	switch ( reconnOpts.strategy ) {
		case 'constant' :
			reOpts.strategy.type = Re.STRATEGIES.CONSTANT ;
			break;
		case 'exponential' :
			reOpts.strategy.type = Re.STRATEGIES.EXPONENTIAL ;
			reOpts.strategy.base = base ;
			break;
		case 'linear' :
		default :
			reOpts.strategy.type = Re.STRATEGIES.LINEAR ;
	}
	
	return reOpts ;

} ;

Connection.prototype._buildSocketOpts = function ( socketOpts ) {
	
	return _.isObject( socketOpts ) ? _.pick( socketOpts , this._sockFields ) : {} ;
	
};

Connection.prototype._buildUrl = function ( suppliedOpts , socketOpts ) {
	
	var amqpOpts = _.isObject(suppliedOpts) ? suppliedOpts : {} ;
	var urlOpts = _.pick( amqpOpts , this._connFields ) ;
	var otherOpts = _.pick( amqpOpts , this._amqFields ) ;
	var protocol = (urlOpts.ssl || (socketOpts && socketOpts.ssl)) ? 'amqps' : 'amqp' ;
	var basicAuth = urlOpts.login ? format( "%s:%s@" , urlOpts.login , urlOpts.password ) : "" ;
	var port = urlOpts.port ? format( ":%s" , urlOpts.port ) : "" ;
	var host = urlOpts.host || 'localhost' ;
	var vhost = urlOpts.vhost ? format( "/%s" , urlOpts.vhost ) : '' ;
	var qs = ! _.isEmpty(otherOpts) ? format( "?%s", querystring.stringify( otherOpts ) ) : '' ;
	
	return format( "%s://%s%s%s%s%s" , protocol , basicAuth , host , port , vhost , qs ) ;
	
} ;

Connection.prototype.exchange = function ( name , options ) {
	
	var self = this ;
	
	return new Exchange( self , name , options ) ;
	
}

Connection.prototype.queue = function ( name , options ) {
	
	var self = this ;
	
	return new Queue( self , name , options ) ;
	
}

Connection.prototype.rpc = function ( exchange ) {
	
	var self = this ;
	
	return new Rpc( self , exchange ) ;
	
}


Connection.prototype.publish = function ( exchangeOrName , routingKey , message , options , callback ) {
	
	var self = this ;
	var m = Buffer.isBuffer( message ) ? message : new Buffer( message.toString() ) ;
	var exg = exchangeOrName instanceof Exchange ? exchangeOrName : exchangeOrName.toString() ;
	
	return self._auxChannelPromise.then(function(ch){ 
		return when.promise(function(resolve,reject){
			var ok = ch.publish( exg , routingKey , m , options , callback ) ;
			if( ok ){
				resolve(true);
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
			}
		})
	});
	
};

Connection.prototype.sendToQueue = function ( queueOrName , message , options , callback ) {
	
	var self = this ;
	var m = Buffer.isBuffer( message ) ? message : new Buffer( message.toString() ) ;
	var queue = queueOrName instanceof Queue ? queueOrName : queueOrName.toString() ;
	
	return self._auxChannelPromise.then(function(ch){ 
		return when.promise(function(resolve,reject){
			var ok = ch.sendToQueue( queue , m , options , callback ) ;
			if( ok ){
				resolve(true);
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
			}
		});
	});
	
};

Connection.prototype.getFromQueue = function ( name, opts ) {
	
	var self = this ;
	
	return self._auxChannelPromise.then(function(ch){ 
		return ch.get(  name , opts  );
	});
	
};

Connection.prototype._connFields = ['host' , 'port' , 'ssl' , 'login' , 'password' , 'vhost' ] ;
Connection.prototype._amqFields = ['frameMax' , 'channelMax' , 'heartbeat' , 'locale' ] ;
Connection.prototype._sockFields = [ 'socket' , 'pfx' , 'key' , 'passphrase' , 'NPNProtocols' , 'servername' , 'secureProtocol' , 'cert' , 'key' , 'ca' , 'rejectUnauthorized' , 'localAddress' , 'path' , 'allowHalfOpen' ]
Connection.prototype._retryFields = [ 'retries' , 'strategy' , 'initial' , 'max' , 'base' ] ;
