import { Producer } from 'node-rdkafka';
import { Order } from './OrderConsumer';

export default class BalconyProducer extends Producer {
  constructor() {
    super({
      'metadata.broker.list': process.env.KAFKA_BROKER_URI || 'localhost:9092',
      'dr_cb': true,
    }, {});
    super.on('ready', () => console.log('Started BalconyProducer'));
  }

  sendOrderToBalcony(order: Order): void {
    super.produce('balcony', null, Buffer.from(JSON.stringify(order)));
    console.log(`Table ${order.table} ${Object.keys(order).includes('food') ? 'food' : 'drinks'} sent to the balcony!`);
  }

  start(): void {
    super.connect();
  }

  close(): void {
    super.disconnect();
  }
}
