var Benchmark = require('benchmark');
var querystring = require('querystring') ;
var fork = require('child_process').fork ;
var when = require('when') ;
var async = require('async') ;
var format = require('util').format ;
var amq = require('../index') ;
var compare = process.argv[2] == 'compare' ;
var doHttp = compare || process.argv[2] == 'http' ;
var doRpc = compare || process.argv[2] == 'rpc' ;
var config = require('./config') ;
var promises = [] ;
var rpcClient ;
var rpcMethod ;
var httpUrl = format( 'http://%s:%s', config.http.host , config.http.port )


if ( doHttp ) {
	
	var httpReady = when.defer() ;
	var httpProc = fork(__dirname+'/http_server.js') ;
	httpProc.once('message', httpReady.resolve );
	promises.push(httpReady.promise);
	process.on('uncaughtException',function(){
	
		httpProc.kill() ;
	
	})
	
}

if ( doRpc ) {
	
	var rpcProc = fork(__dirname+'/rpc_server.js') ;
	var rpcReady = when.defer() ;
	rpcProc.once('message', rpcReady.resolve );
	promises.push(rpcReady.promise.then(function(){
	
		rpcClient = amq.createConnection(config.rpc.connection).rpc(config.rpc.exchange) ;
		rpcMethod = config.rpc.method ;
		return rpcClient.ready() ;
	
	}));
	process.on('uncaughtException',function(){
	
		rpcProc.kill() ;
	
	})
}



when.all(promises).then( runSuite ).then(null,function(err){
	
	console.error(err.stack||err)
	
}) ;

function runSuite () {
	
	var request = require('request') ;	
	var times = 5 ;
	var tests = [] ;
	
	if ( doRpc ) {
		tests.push( rpcNoArgs ) ;
		tests.push( rpcWithArgs ) ;
	}
	
	if ( doHttp ) {
		
		tests.push(httpGet) ;
		tests.push(httpPost) ;
		
	}
	
	async.series(tests,function(){})
	
	function httpGet ( allDone ) {
		
		runTest( 'http#get' , times , function(done){
			
			request({ method: 'GET' , uri: httpUrl  }, function (error, response, body) {
				done(null) ;
	    })
			
		},allDone) ;
		
	}

	function httpPost ( allDone ) {
		
		runTest( 'http#post' , times , function(done){
			
			var postData = querystring.stringify({ name : 'world' }) ;
			request({ method: 'POST' , uri: httpUrl , body : postData  }, function (error, response, body) {
				done(null) ;
	    })
			
		},allDone) ;
		
	}
	
	function rpcNoArgs ( allDone ) {
		
		runTest( 'rpc#noArgs' , times , function(done){
			
			rpcClient.call(rpcMethod,null).then( done , done )
			
		},allDone) ;
		
	}
	
	function rpcWithArgs ( allDone ) {
		
		runTest( 'rpc#args' , times , function(done){
			
			rpcClient.call(rpcMethod,'foo').then( done , done )
			
		},allDone) ;
		
	}

}

function runTest ( name , times , fn , done ) {
	
	var set = 500 ;
	var duration = 0 ;
	var runs = set * times ;
	
	async.timesSeries( times , function ( n1 , doneWithIteration ) {
		
		var start = Date.now() ;
		
		async.times( set , function ( i , cb ) {
			setImmediate(function(){
				fn(cb);				
			})
		},function(){
			
			var end = Date.now() ;
			duration += ( end-start ) ;
			setTimeout(doneWithIteration,1000) ;
			
		})
	
	} , function (){
		
		var hz = runs / ( duration/1000 ) ;
		
		console.log("%s: %s ops/sec",name,hz) ;
		
		done() ;
		
	});
	
}