import { mkdtemp, rm, writeFile, mkdir } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runPipelineFromRepo } from '../../src/core/ingestion/pipeline.js';
import type { PipelineResult } from '../../types/pipeline.js';

describe('Java RocketMQ graph edges', () => {
  let repoPath: string;
  let result: PipelineResult;

  beforeAll(async () => {
    repoPath = await mkdtemp(path.join(os.tmpdir(), 'gitnexus-java-rocketmq-'));
    const srcDir = path.join(repoPath, 'src/main/java/example');
    const resourcesDir = path.join(repoPath, 'src/main/resources');
    await mkdir(srcDir, { recursive: true });
    await mkdir(resourcesDir, { recursive: true });
    await writeFile(
      path.join(resourcesDir, 'application.properties'),
      `
        rocketmq.orderPaid.topic=vip_trade_msg_order_paid
        rocketmq.orderPaid.consumer.group=CG-order-paid
        rocketmq.producers[0].topic=save_book_venue_receive_seat_order_v2
      `,
    );
    await writeFile(
      path.join(srcDir, 'OrderPaidConsumer.java'),
      `
        package example;

        @RocketMQMessageListener(
          topic = "\${rocketmq.orderPaid.topic}",
          consumerGroup = "\${rocketmq.orderPaid.consumer.group}")
        class OrderPaidConsumer implements RocketMQListener<String> {}
      `,
    );
    await writeFile(
      path.join(srcDir, 'SendEventServiceImpl.java'),
      `
        package example;

        class SendEventServiceImpl {
          @Value("\${rocketmq.producers[0].topic:save_book_venue_receive_seat_order}")
          private String topic;
          void send() {
            rocketMQTemplate.syncSend(topic, MessageBuilder.withPayload(body).build());
          }
        }
      `,
    );

    result = await runPipelineFromRepo(repoPath, () => {});
  }, 60000);

  afterAll(async () => {
    if (repoPath) await rm(repoPath, { recursive: true, force: true });
  });

  it('adds RocketMQ topic nodes and producer/consumer edges', () => {
    const topics: string[] = [];
    result.graph.forEachNode((node) => {
      if (node.label === 'CodeElement' && node.properties.kind === 'RocketMQTopic') {
        topics.push(String(node.properties.name));
      }
    });
    expect(topics.sort()).toEqual([
      'save_book_venue_receive_seat_order_v2',
      'vip_trade_msg_order_paid',
    ]);

    const relTypes: string[] = [];
    result.graph.forEachRelationship((rel) => {
      if (rel.type === 'CONSUMES_TOPIC' || rel.type === 'PRODUCES_TOPIC') {
        relTypes.push(`${rel.type}:${rel.reason}`);
      }
    });

    expect(relTypes.sort()).toEqual([
      expect.stringContaining('CONSUMES_TOPIC:java-rocketmq-consumer'),
      expect.stringContaining('PRODUCES_TOPIC:java-rocketmq-producer'),
    ]);
  });
});
