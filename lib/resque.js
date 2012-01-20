var resque = exports;
var EventEmitter = require ('events').EventEmitter;
var sys          = require ('sys');
var redis        = require ('redis');

var processAlive = function (pid, cb) {
    require('child_process').exec (
	"ps -p" + pid + "| wc -l",
	function (error, stdout, stderr) {
	    cb (parseInt (stdout) === 2)
	}
    )
}

var connectToRedis = function (options) {
    return redis.createClient (options.port, options.host);
}


resque.connect = function (options) {
    var options    = options || {}
    var redis      = options.redis || connectToRedis(options)
    var namespace  = options.namespace || 'resque'
    var connection = { redis: redis, namespace: namespace }

    var key = function () {
	var args = Array.prototype.slice.apply (arguments)
	args.unshift (namespace)
	return args.join (":")
    }

    var cleanUpWorker = function (identifier, cb) {
	sys.puts ("cleaning worker "+identifier)
	redis.srem (key ('workers'), identifier, function () {
	    redis.del ([
		key ('worker', identifier)
		, key ('worker', identifier, ':started')
		, key ('stat', 'failed', identifier)
		, key ('stat', 'processed', identifier)
	    ], function () { if (cb) cb () })
	})
    }

    connection.cleanStaleWorkers = function () {
	redis.smembers (key ('workers'), function (err, workers) {
	    if (! workers) return
	    workers.forEach (function (worker) {
		var parts = worker.toString().split(":")
		var pid = parts [1]
		processAlive (pid, function (isAlive) {
		    if (!isAlive) cleanUpWorker (worker.toString ())
		})
	    })
	})
    }
	
    connection.createWorker = function (queues, name, callbacks) {
	var worker = new EventEmitter ()
	, callbacks = callbacks || {}
	, poll = function () {
	    worker.emit ('poll')

	    worker.queue = worker.queues.shift ()
	    worker.queues.push (worker.queue)

	    redis.lpop (key ("queue", worker.queue), function (err, job) {
		if (job)
		    worker.emit ('job', JSON.parse (job.toString ()))
		else
		    redis.del (
			key ('worker', worker.identifier)
			, function () { setTimeout (poll, 1000) }
		    )
	    })
	}

	worker.queues = []

	if (queues === '*')
	    redis.smembers (
		key ('queues'),
		function (err, queues) { worker.queues = queues }
	    )
	else worker.queues =
	    (typeof queues === 'string') ?
	    queues.split (",") :
	    queues

	worker.identifier = [
	    name || 'node', process.pid, queues
	].join (":")

	sys.puts ("[" + namespace + "] " + worker.identifier)

	worker.succeed = function (job) {
	    redis.incr (key ('stat', 'processed'), function () {
		redis.incr (
		    key ('stat', 'processed', worker.identifier)
		    , function () { process.nextTick (poll) }
		)
	    })
	}

	worker.fail = function (job, failure) {
	    var failure = failure || {}
	    redis.rpush (
		key ('failed')
		, JSON.stringify ({
		    worker: worker.identifier
		    , error: failure.error || 'unspecified'
		    , queue : worker.queue
		    , payload: job
		    , exception: failure.exception || 'generic'
		    , backtrace: failure.backtrace || ['unknown']
		    , failed_at: (new Date ()).toString ()
		})
	    )

	    redis.incr (key ('stat', 'failed'))
	    redis.incr (key ('stat', 'failed', worker.identifier))
	    process.nextTick (poll)
	}

	worker.addListener ('job', function (job) {
	    var callback = callbacks [ job.class ]

	    if (typeof callback === 'function') {
		redis.set (
		    key ('worker', worker.identifier)
		    , JSON.stringify ({
			run_at: (new Date ()).toString ()
			, queue: worker.queue
			, payload: job
		    })
		)


		callback.apply ({
		    succeed: function () { worker.succeed (job) }
		    , fail: function (failure) { worker.fail (job, failure) }
		    , worker: worker
		    , class: job.class
		    , args: job.args
		}, job.args)
	    } else
		worker.fail (job, {
		    error: 'No callback for "'+job.class+'"'
		    , exception: "UnknownClass"
		})
	})

	worker.end = function () {
	    cleanUpWorker (worker.identifier, function () {
		worker.emit ('end')
	    })
	}

	worker.finish = function () { poll = worker.end }

	worker.start = function () {
	    redis.sadd (key ('workers'), worker.identifier)

	    redis.set (
		key ('worker', worker.identifier, 'started')
		, (new Date ()).toString ()
	    )

	    poll ()
	    return worker
	}

	return worker
    }

    connection.enqueue = function (queueName, fnName) {
	var args = Array.prototype.slice.call (arguments, 2)
	redis.sadd (key ('queues'), queueName)
	redis.rpush (
	    key ('queue', queueName)
	    , JSON.stringify ({ class: fnName, args: args })
	)
    }

    return connection
}
