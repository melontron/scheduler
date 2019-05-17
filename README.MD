# job-sceduler
## Installation
`npm install @lordmelique/job-scheduler`
```js
const {Scheduler, Consumer} = require('job-scheduler')
```

## Scheduler

First of all we need to declare scheduler and give the job names which will be scheduled by this scheduler.
```js
const scheduler = new Scheduler({
  redis: {
    port: 6378,
  },
  enqueueMaxTries: 5,
  retryInterval: 10,
  types: ['myjob'],
});
```

Then we need to add consumer that will consume AWS SQS queue and fire events.

```js
const consumer = new Consumer({
  jobName: 'myjob',
  sqsUrl: 'https://sqs.<REGION>.amazonaws.com/<ACCOUNT_ID>',
  message: function handleMessage(message) {
    console.log(message.Body)
  },
  error: function handleError(err) {
    console.log(err);
  },
  processingError: function handleProcessingError(err) {
    console.log(err);
  },
})
```

Now we are ready to schedule jobs with scheduler.
Lets generate jobs with random Ids and TTLs
```js
(async function() {
  await sch.wait(5);
  try {
    for (let i = 0; i < 10; i++) {
      await sch.scheduleEvent({
        type: 'myjob',
        id: parseInt(Math.random() * 200),
        ttl: parseInt(Math.random() * 10) + 5,
      });
    }

  } catch (e) {
    console.log(e);
  }
})();
```
