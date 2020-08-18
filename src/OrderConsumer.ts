import { KafkaConsumer } from 'node-rdkafka';
import BalconyProducer from './BalconyProducer';
import { sleep } from 'sleep';

export type Order = { table: number, food: string[] } | { table: number, drinks: string[] };

export default class OrderConsumer extends KafkaConsumer {
  constructor(
    private readonly balconyProducer: BalconyProducer,
    consumerType: 'Cooker' | 'Bartender',
  ) {
    super({
      'group.id': consumerType,
      'metadata.broker.list': process.env.KAFKA_BROKER_URI || 'localhost:9092',
    }, {})

    const topicName = consumerType === 'Cooker' ? 'food' : 'drinks';
    super
      .on('ready', () => {
        super.subscribe([topicName]);
        super.consume();
        console.log(`Started ${consumerType} consumer on topic ${topicName}`);
      })
      .on('rebalance', () => console.log(`Rebalancing ${consumerType} Consumers...`))
      .on('data', async ({ value }) => await this.prepareOrder(JSON.parse(value.toString()) as unknown as Order));
  }

  async prepareOrder(order: Order): Promise<void> {
    const { table, ...rest } = order;
    const timeToPrepare = Math.floor(Math.random() * 7 + 3);
    console.log(`Preparing table ${table} order (will take ${timeToPrepare}s): ${JSON.stringify(Object.values(rest)[0])}`);
    sleep(timeToPrepare);
    console.log(`Finished table ${table} preparing, sending to balcony...`);
    this.balconyProducer.sendOrderToBalcony(order);
  }

  start(): void {
    super.connect();
  }

  close(): void {
    super.disconnect();
  }
}
