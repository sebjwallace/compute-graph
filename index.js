const { NConsumer, NProducer } = require("sinek")
const Stream = require('./Stream');

(async () => {

  const c = new Stream({
    kafkaConfig: {
      'metadata.broker.list': 'localhost:9092',
      'group.id': 'muru-streams',
      'client.id': 'test',
      'enable.auto.commit': true
    },
    sourceTopic: 'test2',
    processor: (message) => {
      console.log('-- stream 3 message --')
      return message
    }
  })

  await c.start()

  const b = new Stream({
    kafkaConfig: {
      'metadata.broker.list': 'localhost:9092',
      'group.id': 'muru-streams',
      'client.id': 'test',
      'enable.auto.commit': true
    },
    sinkTopic: 'test2',
    processor: (message) => {
      console.log('-- stream 2 message --')
      console.log(message.toString('utf8'))
      return message
    }
  })

  await b.start()

  const a = new Stream({
    kafkaConfig: {
      'metadata.broker.list': 'localhost:9092',
      'group.id': 'muru-streams',
      'client.id': 'test',
      'enable.auto.commit': true
    },
    sourceTopic: 'test1',
    processor: (message) => {
      console.log('-- stream 1 message --')
      return 'next stream'
    }
  })

  a.fork(b)

  console.log('connecting consumer')
  await a.start()

  const producer = new NProducer({
    noptions: {
      'metadata.broker.list': 'localhost:9092',
      'group.id': 'muru-streams',
      'client.id': 'test',
      'enable.auto.commit': true
    }
  }, ['test1'])

  console.log('connect producer')
  await producer.connect()

  setInterval(async () => {
    await producer.send('test1', 'test-message')
    console.log('sent message')
  }, 1000)

})();