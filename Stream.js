const { NConsumer, NProducer } = require("sinek")

module.exports = class Stream {

  constructor({
    kafkaConfig,
    processor,
    sourceTopic,
    sinkTopic,
  }){
    this.kafkaConfig = kafkaConfig
    this.processor = processor
    this.sourceTopic = sourceTopic
    this.sinkTopic = sinkTopic
    this.forks = []

    if(sourceTopic){
      this.consumer = new NConsumer(this.sourceTopic, { noptions: this.kafkaConfig })
    }

    if(sinkTopic){
      this.producer = new NProducer({ noptions: this.kafkaConfig }, [this.sinkTopic], this.producePartitionCount)
    }
  }

  async start(){
    await this.startProducer()
    await this.startConsumer()
  }

  async startProducer(){
    if(!this.producer) return

    // this.producer.on('error', kafkaErrorCallback)
    return this.producer.connect()
  }

  async startConsumer(){
    if(!this.consumer) return

    this.consumer.on("message", this.process.bind(this))
    this.consumer.on("error", err => console.log(err))
    await this.consumer.connect(true, {
      asString: false,
      asJSON: false
    })
    console.log('consumer connected')
  }

  pause(){}

  resume(){}

  stop(){}

  async process(message){
    const output = await this.processor(message)

    if(this.producer) {
      this.producer.send(this.sinkTopic, message)
    }

    for (const fork of this.forks){
      fork.process(output)
    }
  }

  fork(stream){
    this.forks.push(stream)
  }

}