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
	
	var _this = this ;
	var debug = _this._debug = amqpOpts.debug ;
	
	_this._url = _this._buildUrl( amqpOpts , sockOpts ) ;
	_this._socketOpts = _this._buildSocketOpts( sockOpts ) ;
	
	_this.on( 'error' , function(error){

		if ( debug ) util.log( format( 'AMQP %s' , error.stack && error.stack || error.toString()  ) )
		_this.ready( false ) ;
		_this.emit('disconnect') ;
		
	});

	_this.on( 'info' , function(info){

		if ( debug ) util.log( format( 'AMQP %s' , info ) ) ;
		
	});

	_this.on( 'close' , function(){
		
		_this.ready( false ) ;
		_this.conn = undefined ;
		_this.emit('disconnect') ;
		
		if ( ! _this._closedManually ) _this.emit('reconnect') ;		
		if ( debug ) util.log( 'AMQP Connection Closed' ) ;
		
	});
	
	_this._initReconnect( _this._buildReconnectOpts( sockOpts ) ) ;
	
	_this.open().then( null , function ( ) { _this.emit('reconnect') }) ;
	
};

Connection.prototype.close = function () {
	
	this._closedManually = true ;
	if ( _this._conn ) this.conn.close() ;
	
}

Connection.prototype.open = function () {
	
	return this._connect() ;
	
}

Connection.prototype._connect = function () {
	
	var _this = this ;
	var url = _this._url ;
	var socketOpts = _this._socketOpts ;
	var def = when.defer() ;
	
	if ( _this._connecting || _this._ready ){
		
		_this.once('open',function(){
			def.resolve() ;
		});
		
		_this.once('error',function(err){
			def.reject(err) ;
		})
		
	} else {
		
		_this._connecting = true ;
		_this._closedManually = false ;
	
		amqplib.connect( url , socketOpts ).then(function(conn) {
		
			_this.conn = conn ;
			conn.on( 'error' , _this.emit.bind( _this , 'error' ) ) ;
			conn.on( 'close' , _this.emit.bind( _this , 'close' ) ) ;
			
			return conn.createConfirmChannel()
			
		}).then(function( channel ){

			_this._auxChannel = channel ;
			_this._connecting = false ;
			_this.ready( true ) ;
			_this.emit('info','Connected to '+ url);
			_this.emit('open') ;
			def.resolve();
			
		}).then( null , function ( err ) {
	
			_this.conn = undefined ;
			_this._connecting = false ;
			_this.emit( 'error' , err ) ;
			_this.ready(false) ;
			def.reject(err) ;
			
		});
				
	}
	
	return def.promise ;
		
};

Connection.prototype._initReconnect = function ( reconnOpts ) {
	
	if ( ! reconnOpts ) return false ;
	var _this = this ;
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
	
	_this.on('reconnect',function(){
		
		var re = new Re(reOpts) ;
		
		re.try(function(retryCount, done){
			
			_this.emit('info', format( 'reconnecting attempt [%s]' , retryCount ) )
			_this._connect()
				.then(function(){ done( null , retryCount ) })
				.then( null , function(err){ done(err) }) ;
				
		},function(err, retryCount){
		  
			if ( err ) {
				_this.emit('info', 'exhausted recconnection attempts' ) ;				
				_this.emit('reconnect_exhausted') ;
			} else {
				_this.emit('info', format( 'reconnected after %s attempts' , retryCount ) ) ;				
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
	
	var _this = this ;
	
	return new Exchange( _this , name , options ) ;
	
}

Connection.prototype.queue = function ( name , options ) {
	
	var _this = this ;
	
	return new Queue( _this , name , options ) ;
	
}

Connection.prototype.rpc = function ( exchangeOrName , exchangeOptions ) {
	
	var _this = this ;
	
	return new RPC( _this , exchangeOrName , exchangeOptions ) ;
	
}

Connection.prototype.publish = function ( exchangeOrName , routingKey , message , options , callback ) {
	
	var _this = this ;
	var m = new Buffer( message ) ;
	var exg = exchangeOrName instanceof Exchange ? exchangeOrName : exchangeOrName.toString() ;
	var def = when.defer();
	
	_this.ready().then(function(){ 

		var ok = _this._auxChannel.publish( exg , routingKey , m , options , callback ) ;

		if ( ok ) {
			def.resolve(true) ;
		}
		
		_this._auxChannel.once('drain',function(){ def.resolve(true) })
		_this._auxChannel.once('error',function(err){ def.reject(err) })
		
	});
	
	return def.promise ;
	
};

Connection.prototype.sendToQueue = function ( queueOrName , message , options , callback ) {
	
	var _this = this ;
	var m = new Buffer( message ) ;
	var queue = queueOrName instanceof Queue ? queueOrName : queueOrName.toString() ;
	var def = when.defer();
	
	_this.ready().then(function(){ 

		var ok = _this._auxChannel.sendToQueue( queue , m , options , callback ) ;

		if ( ok ) {
			def.resolve(true) ;
		}
		
		_this._auxChannel.once('drain',function(){ def.resolve(true) })
		_this._auxChannel.once('error',function(err){ def.reject(err) })
		
	});
	
	return def.promise ;
	
};

Connection.prototype._connFields = ['host' , 'port' , 'ssl' , 'login' , 'password' , 'vhost' ] ;
Connection.prototype._amqFields = ['frameMax' , 'channelMax' , 'heartbeat' , 'locale' ] ;
Connection.prototype._sockFields = [ 'socket' , 'pfx' , 'key' , 'passphrase' , 'NPNProtocols' , 'servername' , 'secureProtocol' , 'cert' , 'key' , 'ca' , 'rejectUnauthorized' , 'localAddress' , 'path' , 'allowHalfOpen' ]
Connection.prototype._retryFields = [ 'retries' , 'strategy' , 'initial' , 'max' , 'base' ] ;
