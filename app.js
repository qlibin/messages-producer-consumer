var redis = require('redis');
var argv = require('minimist')(process.argv.slice(2));
console.dir(argv);

var client = redis.createClient(); //creates a new client 



client.on('connect', function() {
    console.log('connected');

    lock.aquire(function(err, lockId){
        if (err) {
            console.log("error", err);
        } else {
            if (lockId) {
                console.log('log aquired. do producer stuff');
                client.publish("messages", getMessage())
            } else {
                console.log('log not aqured. consume');
                client.subscribe("messages");
            }
        }
    });

    client.on("message", function (channel, message) {
        console.log("client1 channel " + channel + ": " + message);
       
        if (channel === "messages") {
 
            eventHandler(message, function eventHandlerCallback(error, msg) {
                if (error) {
                    client.rpush("errorMessages", msg);
                }
            });

        }

        if (message === "poison") {
            client.unsubscribe();
            client.end();
        }
    });

});

function getMessage(){
    this.cnt = this.cnt || 0;
    return this.cnt++;
}

function eventHandler(msg, callback){
    function onComplete(){
        var error = Math.random() > 0.85;
        callback(error, msg);
    }
    // processing takes time...
    console.log('processing takes time...');
    setTimeout(onComplete, Math.floor(Math.random()*1000));
}


var lock = (function lock() {

    var generateLockId = function lockIdGenerator() {
        return new Date().getTime();
    }, LOCK_TIMEOUT = 30000; //ms

    function aquire(producerId, callback) {

        console.log("trying to aquire lock:producer with pid:" + producerId);

        client.set("lock:producer", "pid:"+producerId, "NX", "PX", LOCK_TIMEOUT, function setLockCallback(err, reply) {
            if (err) {
                console.log("error", err);
                callback(err);
            } else {
                console.log("reply", reply);
                if (reply === "OK" && typeof callback === 'function') {
                    callback(null, producerId);
                } else {
                    callback();
                }
            }
        });
    }

    return {
        aquire: function aqure(arg1, agr2) {

            if (arg1 && typeof arg2 == 'function') {
                aquire(arg1, arg2);
            } else if (typeof arg1 == 'function') {
                aquire(generateLockId(), arg1);
            } else {
                throw new Error('no arguments provided');
            }

        }
    };

})();


