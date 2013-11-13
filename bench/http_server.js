var http = require('http');
var config = require('./config') ;
var opts = config.http ;
var querystring = require('querystring');

http.createServer(function(request, response) {

  response.writeHead(200, "OK", {'Content-Type': 'text/plain'});

  if(request.method == 'POST') {
		
    var queryData = "";
		
    request.on('data', function(data) {
    	queryData += data;
		});
		
		request.on('end',function(){	
			response.end('Hello '+querystring.parse(queryData).name);
		})
		
  } else {
      response.end('Hello World');
  }

}).listen(opts.port,opts.host,function(){
	
	if ( process.send ) return process.send(opts) ;
	console.log(opts) ;
	
});