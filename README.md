amq
===

A  nodejs AMQP implementation built on top of amqplib's channel-oriented api. Connection/Queue/Exchange constructors supporting auto-reconnection and backoff.


## Examples 

###Connection
```javascript
var amq = require('amq');
var connection = amq.createConnection({ host : 'localhost' , debug : true },{ 
	reconnect : { strategy : 'constant' , initial : 1000 } 
});
```
### Some quick examples 

```javascript
	
	var queue = connection.queue( 'someName' , { durable : true }) ;
	// auto generated queue name, exclusive to conn
	var queue = connection.queue( '' , { exclusive : true }) ;
	var exchange = connection.exchange('myExchange',{durable:true,type:'fanout',confirm:true});
	queue.bind(exchange).then(function(){
		return exchange.publish('foo',{deliveryMode:true,mandatory:true, etc...},function confirmCb(){}).then(function(){
			// written to socket.
		})
	}).then(null,handleErr)
	
	queue.bind('exgName').then(function(){
		...
	}).then(null,handleErr)
	queue.unbind('exgName').then(function(){
		...
	}).then(null,handleErr)
	
	queue.consume(function(message){
		// queue.ack(message);
		// que.nack(message);
		// queue.recover()
	}).then(function(subscription){
		// return connection.sendToQueue(queueName,'foo')
		// return queue.publish(message,opts,cbIfConfirmMode)
	}).then(null,done)

	queue.consume({ prefetch : 1, noLocal : true , etc.. },function(message){
		// queue.ack(message);
		// que.nack(message);
		// queue.recover()
	}).then(function(subscription){
		// return connection.sendToQueue(queueName,'foo')
		// return queue.publish(message,opts,cbIfConfirmMode)
	}).then(null,done)

	// cancel a consumer 	
	queue.consume(fn).then(function(subscription){
		return queue.cancel(subscription.consumerTag)
	}).then(null,done)

	// basick get
	queue.publish('foo').then(function(){
		return queue.get({opts...}).then(function(message){
			// will either get a message or 'false'
		})
	}).then(null,done);
	
	// destroy
	queue.destroy().then(function(){
		queue.check().then(null,assert.ifError).then(null,done.bind(null,null));
	}).then(null,done)
	
```
