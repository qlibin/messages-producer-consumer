var logger = require('logger').createLogger();

exports.Lock = Lock;

function Lock(client, name) {

	var generateLockId = function lockIdGenerator() {
			return new Date().getTime();
		},
		LOCK_TIMEOUT = 1000, //ms
		lockName = name || "lock",
		lockId = generateLockId();

	function acquire(success, error) {

		lockId = lockId || generateLockId();

		logger.debug("trying to acquire " + lockName + " with " + lockId);

		var script = "if redis.call(\"get\",KEYS[1]) == ARGV[1] or not redis.call(\"get\", KEYS[1]) then return redis.call(\"set\",KEYS[1],ARGV[1],\"PX\",ARGV[2]) else return 0 end";

		//client.set(lockName, lockId, "NX", "PX", LOCK_TIMEOUT, function setLockCallback(err, reply) {
		client.eval(script, 1, lockName, lockId, LOCK_TIMEOUT, function setLockCallback(err, reply) {
			if (err) {
				logger.debug("lock acquire error", err);
				if (typeof error === 'function') error(err);
			} else {
				logger.debug("reply", reply);
				if (reply === "OK") {
					if (typeof success === 'function') success(lockId);
				} else {
					if (typeof error === 'function') error();
				}
			}
		});
	}

	function release(success, error) {
		var script = "if redis.call(\"get\",KEYS[1]) == ARGV[1] then return redis.call(\"del\",KEYS[1]) else return 0 end";
		client.eval(script, 1, lockName, lockId, function (err, reply) {
			if (err) {
				logger.debug("lock release error", err);
				if (typeof error === 'function') error(err);
			} else {
				if (reply == 1) {
					logger.debug("lock " + lockId + " released");
					if (typeof success === 'function') success();
				} else {
					if (typeof error === 'function') error(err);
				}
			}
		});
	}

	return {
		tryAcquire: acquire,
		releaseLock: release
	};

}
