var events = require('events') ;
var querystring = require('querystring') ;
var util = require('util') ;
var ready = require('ready') ;
var amqplib = require('amqplib') ;
var when = require('when') ;
var _ = require('underscore') ;
var Re = require('re') ;
var format = require('util').format ;
var Exchange = require('./Exchange') ;
var Queue = require('./Queue') ;
var RPC = require('./RPC') ;

util.inherits( Connection , events.EventEmitter ) ;

ready.mixin( Connection.prototype ) ;

module.exports = Connection ;

function Connection ( amqpOpts , sockOpts ) {
	
	var self = this ;
	var debug = self._debug = amqpOpts.debug ;
	
	self._url = self._buildUrl( amqpOpts , sockOpts ) ;
	self._socketOpts = self._buildSocketOpts( sockOpts ) ;
	
	self.on( 'error' , function(error){

		if ( debug ) util.log( format( 'AMQP %s' , error.stack && error.stack || error.toString()  ) )
		self.ready( false ) ;
		self.emit('disconnect') ;
		
	});

	self.on( 'info' , function(info){

		if ( debug ) util.log( format( 'AMQP %s' , info ) ) ;
		
	});

	self.on( 'close' , function(){
		
		self.ready( false ) ;
		self.conn = undefined ;
		self.emit('disconnect') ;
		
		if ( ! self._closedManually ) self.emit('reconnect') ;		
		if ( debug ) util.log( 'AMQP Connection Closed' ) ;
		
	});
	
	self._initReconnect( self._buildReconnectOpts( sockOpts ) ) ;
	
	self.open().then( null , function ( ) { self.emit('reconnect') }) ;
	
};

Connection.prototype.close = function () {
	
	var deferred = when.defer() ;
	deferred.resolve() ;
	this._closedManually = true ;
	
	if ( this._conn ) {
		return deferred.promise.then(function(){
			return this.conn.close() ;
		})
	}
	
	return deferred.promise ;
	
};

Connection.prototype.open = function () {
	
	return this._connect() ;
	
};

Connection.prototype._connect = function () {
	
	var self = this ;
	var url = self._url ;
	var socketOpts = self._socketOpts ;
	var def = when.defer() ;
	
	if ( self._connecting || self._ready ){
		
		self.once('open',function(){
			def.resolve() ;
		});
		
		self.once('error',function(err){
			def.reject(err) ;
		})
		
	} else {
		
		self._connecting = true ;
		self._closedManually = false ;
	
		amqplib.connect( url , socketOpts ).then(function(conn) {
		
			self.conn = conn ;
			conn.on( 'error' , self.emit.bind( self , 'error' ) ) ;
			conn.on( 'close' , self.emit.bind( self , 'close' ) ) ;
			
			return conn.createConfirmChannel()
			
		}).then(function( channel ){

			self._auxChannel = channel ;
			self._connecting = false ;
			self.ready( true ) ;
			self.emit('info','Connected to '+ url);
			self.emit('open') ;
			def.resolve();
			
		}).then( null , function ( err ) {
	
			self.conn = undefined ;
			self._connecting = false ;
			self.emit( 'error' , err ) ;
			self.ready(false) ;
			def.reject(err) ;
			
		});
				
	}
	
	return def.promise ;
		
};

Connection.prototype._initReconnect = function ( reconnOpts ) {
	
	if ( ! reconnOpts ) return false ;
	var self = this ;
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
	
	self.on('reconnect',function(){
		
		var re = new Re(reOpts) ;
		
		re.try(function(retryCount, done){
			
			self.emit('info', format( 'reconnecting attempt [%s]' , retryCount ) )
			self._connect()
				.then(function(){ done( null , retryCount ) })
				.then( null , function(err){ done(err) }) ;
				
		},function(err, retryCount){
		  
			if ( err ) {
				self.emit('info', 'exhausted recconnection attempts' ) ;				
				self.emit('reconnect_exhausted') ;
			} else {
				self.emit('info', format( 'reconnected after %s attempts' , retryCount ) ) ;				
			}
			
		})
		
	});

}

Connection.prototype._buildSocketOpts = function ( socketOpts ) {
	
	return _.isObject( socketOpts ) ? _.pick( socketOpts , this._sockFields ) : {} ;
	
};

Connection.prototype._buildReconnectOpts = function ( socketOpts ) {
	
	return _.isObject( socketOpts ) ? _.pick( socketOpts , this._retryFields ) : null ;
	
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

Connection.prototype.rpc = function ( exchangeOrName , exchangeOptions ) {
	
	var self = this ;
	
	return new RPC( self , exchangeOrName , exchangeOptions ) ;
	
}

Connection.prototype.publish = function ( exchangeOrName , routingKey , message , options , callback ) {
	
	var self = this ;
	var m = Buffer.isBuffer( message ) ? message : new Buffer( message ) ;
	var exg = exchangeOrName instanceof Exchange ? exchangeOrName : exchangeOrName.toString() ;
	var def = when.defer();
	
	self.ready().then(function(){ 

		var ok = self._auxChannel.publish( exg , routingKey , m , options , callback ) ;

		if ( ok ) {
			def.resolve(true) ;
		}
		
		self._auxChannel.once('drain',function(){ def.resolve(true) })
		self._auxChannel.once('error',function(err){ def.reject(err) })
		
	});
	
	return def.promise ;
	
};

Connection.prototype.sendToQueue = function ( queueOrName , message , options , callback ) {
	
	var self = this ;
	var m = Buffer.isBuffer( message ) ? message : new Buffer( message ) ;
	var queue = queueOrName instanceof Queue ? queueOrName : queueOrName.toString() ;
	var def = when.defer();
	
	self.ready().then(function(){ 

		var ok = self._auxChannel.sendToQueue( queue , m , options , callback ) ;

		if ( ok ) {
			def.resolve(true) ;
		}
		
		self._auxChannel.once('drain',function(){ def.resolve(true) })
		self._auxChannel.once('error',function(err){ def.reject(err) })
		
	});
	
	return def.promise ;
	
};

Connection.prototype._connFields = ['host' , 'port' , 'ssl' , 'login' , 'password' , 'vhost' ] ;
Connection.prototype._amqFields = ['frameMax' , 'channelMax' , 'heartbeat' , 'locale' ] ;
Connection.prototype._sockFields = [ 'socket' , 'pfx' , 'key' , 'passphrase' , 'NPNProtocols' , 'servername' , 'secureProtocol' , 'cert' , 'key' , 'ca' , 'rejectUnauthorized' , 'localAddress' , 'path' , 'allowHalfOpen' ]
Connection.prototype._retryFields = [ 'retries' , 'strategy' , 'initial' , 'max' , 'base' ] ;
