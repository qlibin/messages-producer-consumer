var redis = require('redis');
var logger = require('logger').createLogger();
var argv = require('minimist')(process.argv.slice(2));
var Lock = require('./lock').Lock;

if (argv.logLevel) {
    logger.setLevel(argv.logLevel);
}

var client = redis.createClient(); //creates a new client

var SEND_MESSAGE_TIMEOUT = argv.timeout || argv.t || 500;

logger.info("started");

client.on('connect', function onConnect() {

    var lock, sendMsgInterval, producer = false, loopCounter = 0;

    logger.info('connected to redis');

    if (argv._.indexOf('getErrors') > -1) {

        logger.info('read and clear all error messages');
        readAndClearErrorMessages();

    } else {

        lock = new Lock(client, "lock:producer");

        sendMsgInterval = setInterval(produceOrConsume, SEND_MESSAGE_TIMEOUT);

    }

    function produceOrConsume() {
        loopCounter++;
        lock.tryAcquire(function successAcquireHandler() {
            if (!producer) logger.info("I'm a message producer now");
            producer = true;
            logger.debug('lock acquired. do producer stuff');
            sendMessage();
        }, function errorAcquireHandler(err) {
            if (loopCounter == 1) logger.info("I will consume messages");
            producer = false;
            if (err) {
                logger.error(err);
            }
            logger.debug('lock not acquired. consume');
            readMessage();
        });
    }

    function sendMessage() {
        var message = getMessage();
        logger.debug('write message', message);
        client.rpush("messages", message);
    }

    function readMessage() {
        client.lpop("messages", function onMessageRead(err, message) {

            if (err) {
                logger.error('error message receive', err);
            } else {
                if (message !== null) {
                    logger.debug("read message", message);
                    eventHandler(message, function eventHandlerCallback(error, msg) {
                        if (error) {
                            logger.error("message processed with error. store it", message);
                            client.rpush("errorMessages", msg);
                        }
                    });
                } else {
                    logger.debug("no new messages");
                }
            }

        });
    }

    function readAndClearErrorMessages() {
        var multi = client.multi().lrange('errorMessages', 0, -1, function(err, reply) {
            logger.info("Error messages");
            logger.info(reply);
        }).del("errorMessages", function() {
            logger.info("Error messages are deleted");
            client.end();
        }).exec();
    }

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
    logger.debug('processing takes time...');
    setTimeout(onComplete, Math.floor(Math.random()*1000));
}




