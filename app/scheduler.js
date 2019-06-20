const redis = require('redis');
const bluebird = require('bluebird');
const AWS = require('aws-sdk');
const sqs = new AWS.SQS({
  region: process.env.SQS_REGION || 'us-east-1',
});

bluebird.promisifyAll(redis);

class Scheduler {
  constructor(config) {
    this.config = this.setConfig(config);
    this.pub = redis.createClient(this.config.redis);
    this.sub = redis.createClient(this.config.redis);
    this.client = redis.createClient(this.config.redis);
    this.db = this.config.redis.db || 0;

    let queueInitializer = Promise.resolve();

    for (let i = 0; i < this.config.types.length; i++) {
      ((i) => {
        const type = this.config.types[i];
        queueInitializer = queueInitializer
            .then(() => {
              return this._createQueue(type.name);
            }).then((queue) => {
              type.queue = queue.QueueUrl;
              return Promise.resolve();
            });
      })(i);
    }

    queueInitializer.then(() => {
      this.pub.send_command('config', ['set', 'notify-keyspace-events', 'Ex'], (e, r) => {
        this.subscribeExpired(e, r);
      });
    }).catch((e)=>{
      throw e;
    });
  }

  setConfig(config) {
    if (!config.types || (config.types && config.types.length === 0)) {
      throw new Error('At least one job type is required');
    }
    return {
      redis: {
        port: config.redis.port || 6379,
      },
      enqueueMaxTries: config.enqueueMaxTries || 5,
      retryInterval: config.enqueueMaxTries || 10,
      types: config.types.map((item)=>{
        return {
          name: item,
        };
      }),
    };
  }
  subscribeExpired(e, r) {
    const expired_subKey = '__keyevent@' + this.db + '__:expired';
    this.sub.subscribe(expired_subKey, () => {
      this.sub.on('message', async (chan, msg) => {
        const type = this._getType(msg);
        if (!type) return;
        try {
          await this.enqueue(msg, type);
        } catch (e) {
          const parsed = this._parseKey(msg);
          let tries = await this.client.getAsync(this._keyTries(msg));
          tries = parseInt(tries);
          if (tries < this.config.enqueueMaxTries) {
            tries++;
            await this.scheduleEvent({ ...parsed, ttl: this.config.retryInterval, tries: tries });
          } else {
            await this.client.delAsync(this._keyTries(msg));
          }
        }
      });
    });
  }

  async wait(sec) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        resolve();
      }, sec);
    });
  }

  async enqueue(msg, type) {
    const { id } = this._parseKey(msg);
    const params = {
      MessageBody: id, /* required */
      QueueUrl: type.queue, /* required */
      MessageDeduplicationId: id,
      MessageGroupId: id,
    };
    await this.client.delAsync(this._keyTries(msg));
    return sqs.sendMessage(params).promise();
  }

  _parseKey(key) {
    const type = key.split('/')[1];
    const id = key.split('/')[2];
    return { id, type };
  }

  async scheduleEvent({ type, id, ttl, tries }) {
    if (this.config.types.indexOf(type) === -1) {
      throw new Error('Invalid job type');
    }

    tries = tries ? parseInt(tries) : 0;
    const key = this._keyEvent({ type, id });
    await this.client.setAsync(key, id, 'EX', ttl);
    await this.client.setAsync(this._keyTries(key), tries);
  }

  async removeEvent(type, id) {
    const proms = [];
    const key = this._keyEvent({ type, id };
    proms.push(this.client.delAsync(key));
    proms.push(this.client.delAsync(this._keyTries(key)));
    return Promise.all(proms)
  }

  _keyEvent({ type, id }) {
    return `SC/${type}/${id}`;
  }

  _keyTries(msg) {
    return msg + '/tries';
  }

  _getType(key) {
    const type = this._validateType(key.split('/')[1]);
    return type;
  }

  _validateType(type) {
    for (let i = 0; i < this.config.types.length; i++) {
      const cType = this.config.types[i];
      if (cType.name === type) {
        return cType;
      }
    }
    return null;
  }

  _createQueue(name) {
    const params = {
      QueueName: `${name}s.fifo`, /* required */
      Attributes: {
        'MessageRetentionPeriod': `${24 * 3600}`,
        'VisibilityTimeout': `${10 * 60}`,
        'FifoQueue': 'true',
        'ContentBasedDeduplication': 'true',
        /* '<QueueAttributeName>': ... */
      },

    };
    return sqs.createQueue(params).promise();
  }
}

module.exports = Scheduler;


