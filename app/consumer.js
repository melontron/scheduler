const sqsConsumer = require('sqs-consumer');

class Consumer {
  constructor({ message, processingError, error, jobName, sqsUrl }) {
    if (!( message instanceof Function)) {
      throw new Error('message should be a function');
    }
    if (!( processingError instanceof Function)) {
      throw new Error('processingError should be a function');
    }
    if (!( error instanceof Function)) {
      throw new Error('error should be a function');
    }
    if (!jobName) {
      throw new Error('jobName is required');
    }
    this.app = sqsConsumer.Consumer.create({
      queueUrl: `${sqsUrl}/${jobName}s.fifo`,
      handleMessage: message,
    });

    this.app.on('error', error);

    this.app.on('processing_error', processingError);

    this.app.start();
  }
}


module.exports = Consumer;
