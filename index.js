var Connection = require('./lib/Connection') ;

exports.createConnection = function ( amqpOpts , sockOpts ) {
	
	return new Connection( amqpOpts , sockOpts ) ;
	
};