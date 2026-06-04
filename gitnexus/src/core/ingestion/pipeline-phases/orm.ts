/**
 * Phase: orm
 *
 * Processes ORM queries and creates QUERIES edges.
 *
 * Supported ORMs:
 *   - Prisma (TypeScript/JavaScript)
 *   - Supabase (TypeScript/JavaScript)
 *   - MyBatis (Java XML mappers)
 *
 * @deps    parse, scan
 * @reads   allORMQueries (from parse), allPaths (from scan, for XML mapper discovery)
 * @writes  graph (CodeElement nodes, QUERIES edges)
 */

import path from 'node:path';
import { readFile } from 'node:fs/promises';
import { getLanguageFromFilename, SupportedLanguages } from 'gitnexus-shared';
import type { PipelinePhase, PipelineContext, PhaseResult } from './types.js';
import { getPhaseOutput } from './types.js';
import type { ParseOutput } from './parse.js';
import type { ScanOutput } from './scan.js';
import { generateId } from '../../../lib/utils.js';
import type { ExtractedORMQuery } from '../workers/parse-worker.js';
import type { KnowledgeGraph } from '../../graph/types.js';
import { isDev } from '../utils/env.js';

import { logger } from '../../logger.js';
export interface ORMOutput {
  edgesCreated: number;
  modelCount: number;
}

export const ormPhase: PipelinePhase<ORMOutput> = {
  name: 'orm',
  deps: ['parse', 'scan'],

  async execute(
    ctx: PipelineContext,
    deps: ReadonlyMap<string, PhaseResult<unknown>>,
  ): Promise<ORMOutput> {
    const { allORMQueries } = getPhaseOutput<ParseOutput>(deps, 'parse');
    const { allPaths } = getPhaseOutput<ScanOutput>(deps, 'scan');
    const mybatisQueries = await extractMybatisQueries(allPaths, ctx.repoPath);
    const allQueries = [...allORMQueries, ...mybatisQueries];

    if (allQueries.length === 0) {
      return { edgesCreated: 0, modelCount: 0 };
    }

    return processORMQueries(ctx.graph, allQueries);
  },
};

const TABLE_REF_RE =
  /\b(?:FROM|INTO|UPDATE|JOIN)\s+`?([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)`?(?:\s+(?:AS\s+)?\w+)?/gi;
const STMT_TAG_RE =
  /<(select|insert|update|delete)\s[^>]*\bid\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/\1>/gi;
const NAMESPACE_RE = /<mapper\s[^>]*\bnamespace\s*=\s*["']([^"']+)["']/i;

interface MybatisStatement {
  op: 'select' | 'insert' | 'update' | 'delete';
  id: string;
  tables: string[];
}

function extractTablesFromSql(sql: string): string[] {
  const clean = sql.replace(/<!\[CDATA\[([\s\S]*?)]]>/g, '$1');
  const noComments = clean.replace(/<!--[\s\S]*?-->/g, '');

  const tables = new Set<string>();
  let match: RegExpExecArray | null;
  TABLE_REF_RE.lastIndex = 0;
  while ((match = TABLE_REF_RE.exec(noComments)) !== null) {
    const name = match[1].toLowerCase();
    if (name.length >= 2 && !/^(select|dual|values|set)$/.test(name)) {
      tables.add(name);
    }
  }
  return [...tables];
}

function parseMybatisXml(
  content: string,
): { namespace: string; statements: MybatisStatement[] } | null {
  const namespaceMatch = NAMESPACE_RE.exec(content);
  if (!namespaceMatch) return null;

  const statements: MybatisStatement[] = [];
  let match: RegExpExecArray | null;
  STMT_TAG_RE.lastIndex = 0;
  while ((match = STMT_TAG_RE.exec(content)) !== null) {
    const op = match[1].toLowerCase() as MybatisStatement['op'];
    const id = match[2];
    const tables = extractTablesFromSql(match[3]);
    if (tables.length > 0) statements.push({ op, id, tables });
  }

  return { namespace: namespaceMatch[1], statements };
}

function namespaceToFilePath(namespace: string, allPaths: readonly string[]): string | null {
  const outerNamespace = namespace.includes('$')
    ? namespace.slice(0, namespace.indexOf('$'))
    : namespace;
  const rel = `${outerNamespace.replace(/\./g, '/')}.java`;
  return allPaths.find((p) => p.replace(/\\/g, '/').endsWith(rel)) ?? null;
}

async function extractMybatisQueries(
  allPaths: readonly string[],
  repoPath: string,
): Promise<ExtractedORMQuery[]> {
  const xmlPaths = allPaths.filter((p) => p.endsWith('.xml'));
  if (xmlPaths.length === 0) return [];

  const queries: ExtractedORMQuery[] = [];

  for (const xmlPath of xmlPaths) {
    let content: string;
    try {
      content = await readFile(path.join(repoPath, xmlPath), 'utf-8');
    } catch {
      continue;
    }

    if (!content.includes('<mapper') || !content.includes('namespace')) continue;

    const parsed = parseMybatisXml(content);
    if (!parsed || parsed.statements.length === 0) continue;

    const mapperFilePath = namespaceToFilePath(parsed.namespace, allPaths) ?? xmlPath;
    const mapperClassName = parsed.namespace.split('.').pop() ?? '';

    for (const stmt of parsed.statements) {
      for (const table of stmt.tables) {
        queries.push({
          filePath: mapperFilePath,
          orm: 'mybatis',
          model: table,
          method: stmt.op,
          lineNumber: 0,
          mapperId: stmt.id,
          sqlOp: stmt.op,
          mapperClassName,
        });
      }
    }
  }

  if (isDev && queries.length > 0) {
    const mapperCount = new Set(queries.map((q) => q.filePath)).size;
    logger.info(`MyBatis: ${queries.length} table refs across ${mapperCount} mapper files`);
  }

  return queries;
}

function buildMapperMethodIndex(graph: KnowledgeGraph): {
  methodIndex: Map<string, string>;
  filesWithMethods: Set<string>;
} {
  const methodIndex = new Map<string, string>();
  const filesWithMethods = new Set<string>();

  graph.forEachNode((node) => {
    if (!node.id.startsWith('Method:')) return;
    const filePath = node.properties.filePath as string | undefined;
    if (!filePath || !/(?:Mapper|Dao|DAO|Repository)\.java$/.test(filePath)) return;

    filesWithMethods.add(filePath);
    const idBody = node.id.replace(/^Method:/, '');
    const hashIdx = idBody.lastIndexOf('#');
    const withoutArity = hashIdx >= 0 ? idBody.slice(0, hashIdx) : idBody;
    if (!methodIndex.has(withoutArity)) {
      methodIndex.set(withoutArity, node.id);
    }
  });

  return { methodIndex, filesWithMethods };
}

function ensureFileNode(graph: KnowledgeGraph, filePath: string): string {
  const fileId = generateId('File', filePath);
  if (!graph.getNode(fileId)) {
    graph.addNode({
      id: fileId,
      label: 'File',
      properties: {
        name: path.basename(filePath),
        filePath,
        startLine: 1,
        endLine: 1,
        language: getLanguageFromFilename(filePath) ?? SupportedLanguages.Java,
        isExported: false,
      },
    });
  }
  return fileId;
}

function processORMQueries(
  graph: KnowledgeGraph,
  queries: readonly ExtractedORMQuery[],
): ORMOutput {
  const modelNodes = new Map<string, string>();
  const seenEdges = new Set<string>();
  let edgesCreated = 0;
  let xmlOrphansSkipped = 0;
  const { methodIndex: mapperMethodIndex, filesWithMethods } = buildMapperMethodIndex(graph);

  for (const q of queries) {
    const modelKey = `${q.orm}:${q.model}`;
    let modelNodeId = modelNodes.get(modelKey);
    if (!modelNodeId) {
      const candidateIds = [
        generateId('Class', `${q.model}`),
        generateId('Interface', `${q.model}`),
        generateId('CodeElement', `${q.model}`),
      ];
      const existing = candidateIds.find((id) => graph.getNode(id));
      if (existing) {
        modelNodeId = existing;
      } else {
        modelNodeId = generateId('CodeElement', `${q.orm}:${q.model}`);
        graph.addNode({
          id: modelNodeId,
          label: 'CodeElement',
          properties: {
            name: q.model,
            filePath: '',
            description: `${q.orm} model/table: ${q.model}`,
          },
        });
      }
      modelNodes.set(modelKey, modelNodeId);
    }

    let sourceId: string;
    if (q.orm === 'mybatis' && q.mapperId && q.mapperClassName) {
      const indexKey = `${q.filePath}:${q.mapperClassName}.${q.mapperId}`;
      const methodNodeId = mapperMethodIndex.get(indexKey);
      if (methodNodeId) {
        sourceId = methodNodeId;
      } else if (filesWithMethods.has(q.filePath)) {
        xmlOrphansSkipped++;
        continue;
      } else {
        sourceId = ensureFileNode(graph, q.filePath);
      }
    } else {
      sourceId = ensureFileNode(graph, q.filePath);
    }

    const edgeKey = `${sourceId}->${modelNodeId}:${q.method}:${q.mapperId ?? ''}`;
    if (seenEdges.has(edgeKey)) continue;
    seenEdges.add(edgeKey);

    const reason = q.orm === 'mybatis' && q.sqlOp ? `mybatis-${q.sqlOp}` : `${q.orm}-${q.method}`;

    graph.addRelationship({
      id: generateId('QUERIES', edgeKey),
      sourceId,
      targetId: modelNodeId,
      type: 'QUERIES',
      confidence: q.orm === 'mybatis' ? 1.0 : 0.9,
      reason,
    });
    edgesCreated++;
  }

  if (isDev) {
    const orphanNote = xmlOrphansSkipped > 0 ? `, ${xmlOrphansSkipped} XML orphans skipped` : '';
    logger.info(
      `ORM dataflow: ${edgesCreated} QUERIES edges, ${modelNodes.size} models (${queries.length} total refs${orphanNote})`,
    );
  }

  return { edgesCreated, modelCount: modelNodes.size };
}
