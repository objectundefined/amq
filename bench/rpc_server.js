var amq = require('../index') ;
var config = require('./config') ;
var opts = config.rpc ;
var conn = amq.createConnection(opts.connection) ;
var rpc = conn.rpc(opts.exchange)
var format = require('util').format ;
var when = require('when');

rpc.expose(opts.method,function(d){
	
	return when.promise(function(resolve,reject,notify){
		resolve( 'Hello ' + (d||'World') )		
	})

}).then(function(){

	if ( process.send ) return process.send(opts) ;
	console.log(opts) ;
	
})