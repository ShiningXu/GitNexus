/**
 * Integration Tests: ORM Dataflow Detection (Prisma + Supabase)
 */
import { describe, it, expect, beforeAll } from 'vitest';
import path from 'path';
import { runPipelineFromRepo } from '../../src/core/ingestion/pipeline.js';
import type { PipelineResult } from '../../src/types/pipeline.js';

const ORM_REPO = path.resolve(__dirname, '..', 'fixtures', 'orm-repo');

describe('ORM dataflow detection', () => {
  let result: PipelineResult;

  beforeAll(async () => {
    result = await runPipelineFromRepo(ORM_REPO, () => {});
  }, 60000);

  it('creates QUERIES edges for Prisma calls', () => {
    const queryEdges: { source: string; target: string; reason: string }[] = [];
    for (const rel of result.graph.iterRelationships()) {
      if (rel.type === 'QUERIES') {
        const source = result.graph.getNode(rel.sourceId);
        const target = result.graph.getNode(rel.targetId);
        if (source && target) {
          queryEdges.push({
            source: source.properties.filePath || source.properties.name,
            target: target.properties.name,
            reason: rel.reason ?? '',
          });
        }
      }
    }
    const prismaEdges = queryEdges.filter((e) => e.source.includes('prisma-service'));
    const prismaModels = [...new Set(prismaEdges.map((e) => e.target))];
    expect(prismaModels).toContain('user');
    expect(prismaModels).toContain('post');
    const reasons = prismaEdges.map((e) => e.reason);
    expect(reasons.some((r) => r.includes('prisma-findMany'))).toBe(true);
    expect(reasons.some((r) => r.includes('prisma-create'))).toBe(true);
  });

  it('creates QUERIES edges for Supabase calls', () => {
    const queryEdges: { source: string; target: string; reason: string }[] = [];
    for (const rel of result.graph.iterRelationships()) {
      if (rel.type === 'QUERIES') {
        const source = result.graph.getNode(rel.sourceId);
        const target = result.graph.getNode(rel.targetId);
        if (source && target) {
          queryEdges.push({
            source: source.properties.filePath || source.properties.name,
            target: target.properties.name,
            reason: rel.reason ?? '',
          });
        }
      }
    }
    const supabaseEdges = queryEdges.filter((e) => e.source.includes('supabase-service'));
    const supabaseModels = [...new Set(supabaseEdges.map((e) => e.target))];
    expect(supabaseModels).toContain('bookings');
    expect(supabaseModels).toContain('interpreters');
    expect(supabaseModels).toContain('sessions');
    const reasons = supabaseEdges.map((e) => e.reason);
    expect(reasons.some((r) => r.includes('supabase-select'))).toBe(true);
    expect(reasons.some((r) => r.includes('supabase-insert'))).toBe(true);
  });

  it('creates CodeElement nodes for ORM models', () => {
    const codeElements: string[] = [];
    result.graph.forEachNode((n) => {
      if (n.label === 'CodeElement' && n.properties.description?.includes('model/table')) {
        codeElements.push(n.properties.name);
      }
    });
    expect(codeElements).toContain('user');
    expect(codeElements).toContain('post');
    expect(codeElements).toContain('bookings');
    expect(codeElements).toContain('interpreters');
    expect(codeElements).toContain('sessions');
  });

  it('creates QUERIES edges for MyBatis XML mapper statements', () => {
    const queryEdges: { source: string; target: string; reason: string }[] = [];
    for (const rel of result.graph.iterRelationships()) {
      if (rel.type === 'QUERIES') {
        const source = result.graph.getNode(rel.sourceId);
        const target = result.graph.getNode(rel.targetId);
        if (source && target && rel.reason?.startsWith('mybatis-')) {
          queryEdges.push({
            source: source.properties.filePath || source.properties.name,
            target: target.properties.name,
            reason: rel.reason ?? '',
          });
        }
      }
    }

    const tables = [...new Set(queryEdges.map((e) => e.target))];
    expect(tables).toContain('order_info');
    expect(tables).toContain('order_item');

    const reasons = queryEdges.map((e) => e.reason);
    expect(reasons.some((r) => r === 'mybatis-select')).toBe(true);
    expect(reasons.some((r) => r === 'mybatis-insert')).toBe(true);
    expect(reasons.some((r) => r === 'mybatis-update')).toBe(true);
    expect(reasons.some((r) => r === 'mybatis-delete')).toBe(true);
  });

  it('creates CodeElement nodes for MyBatis tables', () => {
    const mybatisNodes: string[] = [];
    result.graph.forEachNode((n) => {
      if (n.label === 'CodeElement' && n.properties.description?.includes('mybatis')) {
        mybatisNodes.push(n.properties.name);
      }
    });

    expect(mybatisNodes).toContain('order_info');
    expect(mybatisNodes).toContain('order_item');
  });

  it('links MyBatis edges to mapper method nodes when available', () => {
    const methodSources: string[] = [];
    for (const rel of result.graph.iterRelationships()) {
      if (rel.type === 'QUERIES' && rel.reason?.startsWith('mybatis-')) {
        const source = result.graph.getNode(rel.sourceId);
        if (source?.label === 'Method') {
          methodSources.push(source.properties.name);
        }
      }
    }

    expect(methodSources).toEqual(
      expect.arrayContaining(['selectByPrimaryKey', 'insert', 'updateStatus', 'deleteById']),
    );
  });

  it('extracts table names from CDATA-wrapped SQL and ignores XML comments', () => {
    const cdataEdges: string[] = [];
    for (const rel of result.graph.iterRelationships()) {
      if (rel.type === 'QUERIES' && rel.reason?.startsWith('mybatis-')) {
        const target = result.graph.getNode(rel.targetId);
        if (target?.properties.name === 'cdata_table') {
          cdataEdges.push(rel.reason ?? '');
        }
      }
    }

    expect(cdataEdges.length).toBeGreaterThan(0);

    const commentTable = [];
    for (const rel of result.graph.iterRelationships()) {
      if (rel.type === 'QUERIES') {
        const target = result.graph.getNode(rel.targetId);
        if (target?.properties.name === 'ignored_in_comment') {
          commentTable.push(target.properties.name);
        }
      }
    }
    expect(commentTable).toHaveLength(0);
  });

  it('creates a valid XML file source node when namespace cannot be resolved', () => {
    let xmlPathEdgeFound = false;
    for (const rel of result.graph.iterRelationships()) {
      if (rel.type === 'QUERIES' && rel.reason?.startsWith('mybatis-')) {
        const source = result.graph.getNode(rel.sourceId);
        const target = result.graph.getNode(rel.targetId);
        if (source && target?.properties.name === 'cdata_table') {
          expect(source.label).toBe('File');
          expect(source.properties.filePath).toBe('src/main/resources/mapper/CdataMapper.xml');
          xmlPathEdgeFound = true;
        }
      }
    }

    expect(xmlPathEdgeFound).toBe(true);
  });
});
