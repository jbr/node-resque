var resque = exports
  , redis = require ('redis').createClient()
  , EventEmitter = require ('events').EventEmitter
  , sys = require ('sys')
  
  , key = function () {
    var args = Array.prototype.slice.apply (arguments)
    args.unshift (resque.namespace)
    return args.join (":") }

resque.withNamespace = function (name) {
  resque.namespace = name
  return resque }

resque.namespace = "resque"

resque.createWorker = function (queues, callbacks) {
  var worker = new EventEmitter ()
    , callbacks = callbacks || {}
    , poll = function () {
      worker.emit ('poll')
      
      worker.queue = worker.queues.shift ()
      worker.queues.push (worker.queue)
      
      redis.lpop (key ("queue", worker.queue), function (err, job) {
        if (job) worker.emit ('job'
          , JSON.parse (job.toString ()))
        else {
          redis.del (key ('worker', worker.identifier))
          setTimeout (poll, 1000) } }) }

  worker.queues = []
  
  if (queues === '*')
    redis.smembers (key ('queues'), function (err, queues) {
      worker.queues = queues })
  else
    worker.queues = (typeof queues === 'string') ? queues.split (",") : queues
  
  worker.identifier = ['node', process.pid, queues].join (":")

  sys.puts ("[" + resque.namespace + "] " + worker.identifier)
  
  worker.succeed = function (job) {
    redis.incr (key ('stat', 'processed'), function () {
      redis.incr (key ('stat', 'processed', worker.identifier)
        , function () { process.nextTick (poll) }) }) }
  
  worker.fail = function (job, failure) {
    var failure = failure || {}
    redis.rpush (key ('failed')
      , JSON.stringify (
        { worker: worker.identifier
        , error: failure.error || 'unspecified'
        , payload: job
        , exception: failure.exception || 'generic'
        , backtrace: failure.backtrace || ['unknown']
        , failed_at: (new Date ()).toString ()
        }))
    redis.incr (key ('stat', 'failed'))
    redis.incr (key ('stat', 'failed', worker.identifier))
    process.nextTick (poll) }

  worker.addListener('job', function (job) {
    var callback = callbacks [job.class]

    if (typeof callback === 'function') {
      redis.set (key ('worker', worker.identifier)
        , JSON.stringify (
          { run_at: (new Date ()).toString ()
          , queue: worker.queue
          , payload: job
          }))
      callback.apply (
        { succeed: function () { worker.succeed (job) }
        , fail: function (failure) { worker.fail (job, failure) }
        , worker: worker
        , class: job.class
        , args: job.args
        }
        , job.args) }
    else worker.fail (job,
      { error: 'No callback for "'+job.class+'"'
      , exception: "UnknownClass"
      }) })
  
  worker.end = function () {
    poll = function () {
      redis.srem (key ('workers'), worker.identifier, function () {
        redis.del (
          [ key ('worker', worker.identifier)
          , key ('worker', worker.identifier, ':started')
          , key ('stat', 'failed', worker.identifier)
          , key ('stat', 'processed', worker.identifier)
          ]
          , function () {
            sys.puts ("END")
            process.exit () }) }) } }
  
  worker.start = function () {
    'SIGINT SIGHUP'.split(" ").forEach (function (event) {
      process.addListener (event, function () { worker.end () }) })

    redis.sadd (key ('workers'), worker.identifier)
    redis.set (key ('worker', worker.identifier, 'started')
      , (new Date ()).toString ())

    poll ()
    return worker }

  return worker }

resque.enqueue = function (queueName, fnName) {
  var args = Array.prototype.slice.call (arguments, 2)
  redis.sadd (key ('queues'), queueName)
  redis.rpush (key ('queue', queueName)
    , JSON.stringify ({ class: fnName, args: args })) }
