var amq = require('../index') ;
var config = require('./config') ;
var opts = config.rpc ;
var rpc = amq.createConnection(opts.connection).rpc(opts.exchange)
var format = require('util').format ;

rpc.expose(opts.method,function(d){
	
	this.deferred.resolve( 'Hello ' + (d||'World') )

}).then(function(){

	if ( process.send ) return process.send(opts) ;
	console.log(opts) ;
	
})