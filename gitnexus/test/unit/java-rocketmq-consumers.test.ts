import { describe, expect, it } from 'vitest';
import {
  extractJavaRocketMqBeanProperties,
  extractJavaRocketMqConfig,
  extractJavaRocketMqConsumerEdges,
} from '../../src/core/ingestion/route-extractors/java-rocketmq-consumers.js';

describe('Java RocketMQ topic edge extraction', () => {
  it('extracts Spring listener consumers through properties placeholders', () => {
    const config = extractJavaRocketMqConfig([
      {
        filePath: 'src/main/resources/application-prod.properties',
        content: `
          rocketmq.orderPaid.topic=vip_trade_msg_order_paid
          rocketmq.orderPaid.consumer.group=CG-vip-cloud-party-order-paid
        `,
      },
    ]);
    const source = `
      @RocketMQMessageListener(
        topic = "\${rocketmq.orderPaid.topic}",
        consumerGroup = "\${rocketmq.orderPaid.consumer.group}")
      class OrderPaidConsumer implements RocketMQListener<String> {}
    `;

    expect(extractJavaRocketMqConsumerEdges('src/OrderPaidConsumer.java', source, config)).toEqual([
      expect.objectContaining({
        role: 'consumer',
        topicName: 'vip_trade_msg_order_paid',
        consumerGroup: 'CG-vip-cloud-party-order-paid',
        framework: 'rocketmq-spring-listener',
      }),
    ]);
  });

  it('extracts DefaultMQPushConsumer.subscribe topics from @Value fields', () => {
    const config = extractJavaRocketMqConfig([
      {
        filePath: 'application.properties',
        content: `
          basisData.contentProduct.mq.consumer.topic=contentProduct
          basisData.contentProduct.mq.consumer.group=CG-content-product
        `,
      },
    ]);
    const source = `
      class RMQConsumerConfig {
        @Value("\${basisData.contentProduct.mq.consumer.topic}")
        private String contentProductConsumerTopic;
        @Value("\${basisData.contentProduct.mq.consumer.group}")
        private String contentProductConsumerGroup;
        DefaultMQPushConsumer initConsumer() {
          DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(contentProductConsumerGroup);
          consumer.subscribe(contentProductConsumerTopic, "*");
          return consumer;
        }
      }
    `;

    expect(extractJavaRocketMqConsumerEdges('src/RMQConsumerConfig.java', source, config)).toEqual([
      expect.objectContaining({
        role: 'consumer',
        topicName: 'contentProduct',
        consumerGroup: 'CG-content-product',
        framework: 'rocketmq-client-subscribe',
      }),
    ]);
  });

  it('extracts RocketMQProperty bean consumers across files', () => {
    const config = extractJavaRocketMqConfig([
      {
        filePath: 'application.properties',
        content: `
          order.finish.consumer.topic=vip_trade_msg_order_finished
          order.finish.consumer.groupName=CG-vip_interact_order_finished
        `,
      },
    ]);
    const beanSource = `
      class RmqConsumerPropertyConfiguration {
        @Value("\${order.finish.consumer.topic}")
        private String orderFinishTopic;
        @Value("\${order.finish.consumer.groupName}")
        private String orderFinishGroupName;
        @Bean
        public RocketMQProperty orderFinishConsumerProperty() {
          RocketMQProperty consumerProperty = new RocketMQProperty();
          consumerProperty.setTopic(orderFinishTopic);
          consumerProperty.setGroupName(orderFinishGroupName);
          return consumerProperty;
        }
      }
    `;
    const beans = extractJavaRocketMqBeanProperties(
      [{ filePath: 'src/RmqConsumerPropertyConfiguration.java', content: beanSource }],
      config,
    );
    const consumerSource = `
      class RocketMQConfiguration {
        DefaultMQPushConsumer orderFinishConsumer() {
          return handleConsumer(orderFinishConsumerProperty, orderFinishListenerProcessor);
        }
      }
    `;

    expect(
      extractJavaRocketMqConsumerEdges(
        'src/RocketMQConfiguration.java',
        consumerSource,
        config,
        beans,
      ),
    ).toEqual([
      expect.objectContaining({
        role: 'consumer',
        topicName: 'vip_trade_msg_order_finished',
        consumerGroup: 'CG-vip_interact_order_finished',
        framework: 'rocketmq-client-factory',
      }),
    ]);
  });

  it('extracts RocketMQTemplate producers and RocketMQ Message constructors', () => {
    const config = extractJavaRocketMqConfig([
      {
        filePath: 'application.properties',
        content: `
          rocketmq.producers[0].topic=save_book_venue_receive_seat_order_v2
          vcc.data.change.producer.topic=vcc_data_change_notify
        `,
      },
    ]);
    const source = `
      class SendEventServiceImpl {
        @Value("\${rocketmq.producers[0].topic:save_book_venue_receive_seat_order}")
        private String topic;
        @Value("\${vcc.data.change.producer.topic}")
        private String dataChangeTopic;
        void send() {
          rocketMQTemplate.syncSend(topic, MessageBuilder.withPayload(body).build());
          Message msg = new Message(dataChangeTopic, "update", "key", bodyBytes);
          mqProducer.send(msg);
        }
      }
    `;

    const edges = extractJavaRocketMqConsumerEdges('src/SendEventServiceImpl.java', source, config);
    expect(edges.map((e) => `${e.role}:${e.topicName}`).sort()).toEqual([
      'producer:save_book_venue_receive_seat_order_v2',
      'producer:vcc_data_change_notify',
    ]);
  });

  it('extracts custom RocketMessage.setTopic producers through @Value constants', () => {
    const source = `
      class CloudConfigMsgConsumeHandler {
        @Value("\${cloud.config.producer.topic:app_audit_feedback}")
        private String CLOUD_CONFIG_MQ_TOPIC;
        void feedback() {
          RocketMessage<String> rocketMessage = new RocketMessage<>();
          rocketMessage.setTopic(CLOUD_CONFIG_MQ_TOPIC);
          rocketMQSender.send(rocketMessage, cloudConfigMQProducer);
        }
      }
    `;

    expect(
      extractJavaRocketMqConsumerEdges('src/CloudConfigMsgConsumeHandler.java', source),
    ).toEqual([
      expect.objectContaining({
        role: 'producer',
        topicName: 'app_audit_feedback',
        framework: 'rocketmessage-set-topic',
      }),
    ]);
  });
});
