import { mkdtemp, rm, writeFile, mkdir } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runPipelineFromRepo } from '../../src/core/ingestion/pipeline.js';
import type { PipelineResult } from '../../types/pipeline.js';

describe('Java HTTP consumer graph edges', () => {
  let repoPath: string;
  let result: PipelineResult;

  beforeAll(async () => {
    repoPath = await mkdtemp(path.join(os.tmpdir(), 'gitnexus-java-http-'));
    const srcDir = path.join(repoPath, 'src/main/java/example');
    await mkdir(srcDir, { recursive: true });
    await writeFile(
      path.join(srcDir, 'OutboundApi.java'),
      `
        package example;

        import org.springframework.beans.factory.annotation.Value;
        import org.springframework.http.HttpMethod;
        import org.springframework.web.client.RestTemplate;
        import org.springframework.web.util.UriComponentsBuilder;
        import java.net.URI;

        class OutboundApi {
          @Value("\${flow.query.url:http://vip-operation-flow-online/flow-api/flow/query}")
          private String flowQueryUrl;
          private RestTemplate restTemplate;

          void query() {
            UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(flowQueryUrl);
            URI uri = builder.build().toUri();
            restTemplate.exchange(uri, HttpMethod.GET, null, String.class);
          }
        }
      `,
    );

    result = await runPipelineFromRepo(repoPath, () => {});
  }, 60000);

  afterAll(async () => {
    if (repoPath) await rm(repoPath, { recursive: true, force: true });
  });

  it('adds consumer-only Route nodes and FETCHES edges for Java clients', () => {
    const routes: string[] = [];
    result.graph.forEachNode((node) => {
      if (node.label === 'Route') routes.push(String(node.properties.name));
    });
    expect(routes).toContain('/flow-api/flow/query');

    const fetchEdges = [];
    result.graph.forEachRelationship((rel) => {
      if (rel.type === 'FETCHES') fetchEdges.push(rel);
    });
    expect(fetchEdges).toEqual([
      expect.objectContaining({
        reason: expect.stringContaining('java-http-consumer|method:GET'),
      }),
    ]);
    expect(fetchEdges[0].reason).toContain('url:/flow-api/flow/query');
    expect(fetchEdges[0].reason).not.toContain('vip-operation-flow-online');
  });
});
