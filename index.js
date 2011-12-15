/**
 * Module dependencies.
 */

var riak = require('riak-js');

/**
 * Return the `RiakStore` extending `connect`'s session Store.
 *
 * @param {object} connect
 * @return {Function}
 * @api public
 */

module.exports = function(connect) {

  /**
   * Connect's Store.
   */

  var Store = connect.session.Store;

  /**
   * Initialize RiakStore with the given `options`.
   *
   * @param {Object} options
   * @api public
   */

  function RiakStore(options) {
    options = options || {};
    Store.call(this, options);
    this.client = options.client || new riak.getClient(options);
    this.bucket = options.bucket || '_sessions';
    this.dbOptions = { encodeUri: true, debug: false };
    
    if (require('cluster').isMaster && options.reapInterval > 0) {
      setInterval(function() {
        reapSessions(this);
      }.bind(this), options.reapInterval);
    }
    
  };

  /**
   * Reap sessions, ignoring errors
   *
   * @param {Object} this
   * @api public
   */
  function reapSessions(self) {
    var now = new Date();
    self.client
      .add({
        bucket: self.bucket,
        index: "expire_bin",
        start: "1977-08-01T00:00:00.000Z",
        end: now.toJSON()
      })
      .map(function(v) {
        return [v.key];
      })
      .reduce('Riak.filterNotFound')
      .run(function(err, expired) {
        if (!err && expired && expired.unshift) {
          expired.forEach(function(e) {
            self.client.remove(self.bucket, e, self.dbOptions);
          });
        }
      });
  };
  
  /**
   * Inherit from `Store`.
   */

  RiakStore.prototype.__proto__ = Store.prototype;

  /**
   * Attempt to fetch session by the given `sid`.
   *
   * @param {String} sid
   * @param {Function} callback
   * @api public
   */

  RiakStore.prototype.get = function(sid, callback) {
    this.client.get(this.bucket, sid, this.dbOptions, function(err, data, meta) {
      if (err && err.notFound) return callback();
      if (err) return callback(err);
      callback(null, data);
    });
  };

  /**
   * Commit the given `session` object associated with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} session
   * @param {Function} callback
   * @api public
   */

  RiakStore.prototype.set = function(sid, session, callback) {    
    var opt = {
    };
    for (var k in this.dbOptions) {
      opt[k] = this.dbOptions[k];
    }
    var d = new Date(session.cookie._expires);
    if (d) {
      opt.headers = {};
      opt.headers['X-Riak-index-expire_bin'] = d.toJSON();
    }

    this.client.save(this.bucket, sid, session, opt, callback);
  };

  /**
   * Destroy the session associated with the given `sid`.
   *
   * @param {String} sid
   * @api public
   */

  RiakStore.prototype.destroy = function(sid, callback) {
    this.client.remove(this.bucket, sid, this.dbOptions, callback);
  };

  /**
   * Fetch number of sessions.
   *
   * @param {Function} callback
   * @api public

   This is very expensive in riak, so we disable it
  RiakStore.prototype.length = function(callback) {
    this.client.count(this.bucket, callback);
  };
   */

  return RiakStore;
};
