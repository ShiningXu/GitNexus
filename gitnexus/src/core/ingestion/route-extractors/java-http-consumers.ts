import { generateId } from '../../../lib/utils.js';
import type { KnowledgeGraph } from '../../graph/types.js';

export interface JavaHttpConsumerCall {
  filePath: string;
  rawUrl: string;
  routePath: string;
  httpMethod: string;
  lineNumber: number;
  framework: string;
}

const URL_LIKE_RE = /^(?:https?:\/\/|\/)/i;

const DIRECT_METHOD_TO_HTTP: Record<string, string> = {
  getForObject: 'GET',
  getForEntity: 'GET',
  getForResponseEntity: 'GET',
  postForObject: 'POST',
  postForEntity: 'POST',
  postForResponseEntity: 'POST',
  postForResponseString: 'POST',
  postForTextHtmlGBKResEntity: 'POST',
  postJsonForResponseEntity: 'POST',
  postObjectJsonForResponseEntity: 'POST',
  put: 'PUT',
  delete: 'DELETE',
  patchForObject: 'PATCH',
};

const CALL_METHOD_RE =
  /\.(exchange|getForObject|getForEntity|getForResponseEntity|postForObject|postForEntity|postForResponseEntity|postForResponseString|postForTextHtmlGBKResEntity|postJsonForResponseEntity|postObjectJsonForResponseEntity|put|delete|patchForObject|post)\s*\(/g;

type UrlBindings = Map<string, string[]>;

export function normalizeJavaHttpConsumerRoute(rawUrl: string): string | null {
  let s = stripJavaStringLiteral(rawUrl).trim();
  if (!s) return null;
  s = s.replace(/\$\{[^}]+\}/g, '{param}');

  if (/^https?:\/\//i.test(s)) {
    try {
      s = new URL(s).pathname;
    } catch {
      s = s.replace(/^https?:\/\/[^/]+/i, '');
    }
  }

  s = s.split('?')[0].replace(/\/+$/, '');
  if (!s.startsWith('/')) return null;
  return s || '/';
}

export function extractJavaHttpConsumerCalls(
  filePath: string,
  content: string,
): JavaHttpConsumerCall[] {
  const bindings = collectInitialUrlBindings(content);
  propagateSimpleUrlBindings(content, bindings);

  const calls: JavaHttpConsumerCall[] = [];
  const seen = new Set<string>();

  for (const match of content.matchAll(CALL_METHOD_RE)) {
    const methodName = match[1];
    if (!methodName) continue;
    const openParen = match.index + match[0].length - 1;
    const args = splitTopLevelArgs(content, openParen);
    if (args.length === 0) continue;

    const httpMethod =
      methodName === 'exchange' ? inferExchangeHttpMethod(args[1]) : inferHttpMethod(methodName);
    if (!httpMethod) continue;

    const urls = resolveUrlExpression(args[0] ?? '', bindings);
    if (urls.length === 0) continue;

    const lineNumber = lineForIndex(content, match.index) + 1;
    const framework = methodName === 'post' ? 'java-http-wrapper' : 'spring-rest-template';
    for (const rawUrl of urls) {
      const routePath = normalizeJavaHttpConsumerRoute(rawUrl);
      if (!routePath) continue;
      const key = `${filePath}:${lineNumber}:${httpMethod}:${routePath}:${rawUrl}`;
      if (seen.has(key)) continue;
      seen.add(key);
      calls.push({ filePath, rawUrl, routePath, httpMethod, lineNumber, framework });
    }
  }

  return calls;
}

export function processJavaHttpConsumerRoutes(
  graph: KnowledgeGraph,
  calls: readonly JavaHttpConsumerCall[],
): void {
  for (const call of calls) {
    const sourceId = generateId('File', call.filePath);
    const routeNodeId = generateId('Route', call.routePath);
    if (!graph.getNode(routeNodeId)) {
      graph.addNode({
        id: routeNodeId,
        label: 'Route',
        properties: {
          name: call.routePath,
          filePath: call.filePath,
          source: 'java-http-consumer',
          rawUrl: call.rawUrl,
        },
      });
    }

    graph.addRelationship({
      id: generateId(
        'FETCHES',
        `${sourceId}->${routeNodeId}:${call.httpMethod}:${call.lineNumber}`,
      ),
      sourceId,
      targetId: routeNodeId,
      type: 'FETCHES',
      confidence: 0.7,
      reason: `java-http-consumer|method:${call.httpMethod}|framework:${call.framework}|url:${call.rawUrl}`,
    });
  }
}

function collectInitialUrlBindings(content: string): UrlBindings {
  const bindings: UrlBindings = new Map();

  const valueFieldRe =
    /@Value\s*\(\s*"([^"]+)"\s*\)\s*(?:\r?\n\s*(?:@\w+(?:\([^)]*\))?\s*)?)*\s*(?:private|protected|public)?\s*(?:static\s+)?(?:final\s+)?String\s+([A-Za-z_$][\w$]*)/g;
  for (const match of content.matchAll(valueFieldRe)) {
    const valueSpec = match[1];
    const name = match[2];
    if (!valueSpec || !name) continue;
    const urls = extractUrlsFromValueSpec(valueSpec);
    if (urls.length > 0) mergeBinding(bindings, name, urls);
  }

  const literalVarRe = /\b(?:String|URI)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:"([^"]+)"|'([^']+)')\s*;/g;
  for (const match of content.matchAll(literalVarRe)) {
    const name = match[1];
    const value = match[2] ?? match[3];
    if (name && value && URL_LIKE_RE.test(value)) mergeBinding(bindings, name, [value]);
  }

  return bindings;
}

function propagateSimpleUrlBindings(content: string, bindings: UrlBindings): void {
  let changed = true;
  for (let pass = 0; pass < 4 && changed; pass++) {
    changed = false;

    const simpleAssignRe = /\b(?:String|URI|var)\s+([A-Za-z_$][\w$]*)\s*=\s*([^;\n]+);/g;
    for (const match of content.matchAll(simpleAssignRe)) {
      const name = match[1];
      const expr = match[2];
      if (!name || !expr || bindings.has(name)) continue;
      const urls = resolveUrlExpression(expr, bindings);
      if (urls.length > 0) {
        mergeBinding(bindings, name, urls);
        changed = true;
      }
    }

    const builderRe =
      /\b(?:UriComponentsBuilder|var)\s+([A-Za-z_$][\w$]*)\s*=\s*UriComponentsBuilder\.fromUriString\s*\(([^)]+)\)/g;
    for (const match of content.matchAll(builderRe)) {
      const name = match[1];
      const expr = match[2];
      if (!name || !expr || bindings.has(name)) continue;
      const urls = resolveUrlExpression(expr, bindings);
      if (urls.length > 0) {
        mergeBinding(bindings, name, urls);
        changed = true;
      }
    }

    const uriFromBuilderRe =
      /\b(?:URI|var)\s+([A-Za-z_$][\w$]*)\s*=\s*([A-Za-z_$][\w$]*)(?:\.[A-Za-z_$][\w$]*\([^)]*\))*\.toUri\s*\(\s*\)/g;
    for (const match of content.matchAll(uriFromBuilderRe)) {
      const name = match[1];
      const builder = match[2];
      if (!name || !builder || bindings.has(name)) continue;
      const urls = bindings.get(builder) ?? [];
      if (urls.length > 0) {
        mergeBinding(bindings, name, urls);
        changed = true;
      }
    }

    const uriCreateRe =
      /\b(?:URI|var)\s+([A-Za-z_$][\w$]*)\s*=\s*URI\.(?:create|createURI)\s*\(([^)]+)\)/g;
    for (const match of content.matchAll(uriCreateRe)) {
      const name = match[1];
      const expr = match[2];
      if (!name || !expr || bindings.has(name)) continue;
      const urls = resolveUrlExpression(expr, bindings);
      if (urls.length > 0) {
        mergeBinding(bindings, name, urls);
        changed = true;
      }
    }
  }
}

function extractUrlsFromValueSpec(valueSpec: string): string[] {
  const defaults = [...valueSpec.matchAll(/\$\{[^:}]+:([^}]+)\}/g)]
    .map((m) => m[1]?.trim() ?? '')
    .filter((v) => URL_LIKE_RE.test(v));
  if (defaults.length > 0) return defaults;
  return URL_LIKE_RE.test(valueSpec) ? [valueSpec] : [];
}

function resolveUrlExpression(
  expr: string,
  bindings: ReadonlyMap<string, readonly string[]>,
): string[] {
  const concatenated = resolveConcatenatedUrlExpression(expr, bindings);
  if (concatenated.length > 0) return concatenated;

  const out: string[] = [];
  const add = (values: readonly string[]) => {
    for (const value of values) if (!out.includes(value)) out.push(value);
  };

  const literal = firstStringLiteral(expr);
  if (literal && URL_LIKE_RE.test(literal)) add([literal]);

  for (const id of expr.match(/\b[A-Za-z_$][\w$]*\b/g) ?? []) {
    add(bindings.get(id) ?? []);
  }

  return out;
}

function resolveConcatenatedUrlExpression(
  expr: string,
  bindings: ReadonlyMap<string, readonly string[]>,
): string[] {
  const parts = splitTopLevelPlus(expr);
  if (parts.length <= 1) return [];

  let combined = [''];
  for (const part of parts) {
    const values = resolveUrlPart(part, bindings);
    if (values.length === 0) return [];
    const next: string[] = [];
    for (const prefix of combined) {
      for (const value of values) {
        const joined = `${prefix}${value}`;
        if (!next.includes(joined)) next.push(joined);
      }
    }
    combined = next;
  }

  return combined.filter((value) => URL_LIKE_RE.test(value));
}

function resolveUrlPart(
  rawPart: string,
  bindings: ReadonlyMap<string, readonly string[]>,
): string[] {
  const part = stripWrappingParens(rawPart.trim());
  if (!part) return [];

  const literal = firstStringLiteral(part);
  if (literal !== null) return [literal];

  if (/^[A-Za-z_$][\w$]*$/.test(part)) return [...(bindings.get(part) ?? [])];

  const out: string[] = [];
  for (const id of part.match(/\b[A-Za-z_$][\w$]*\b/g) ?? []) {
    for (const value of bindings.get(id) ?? []) {
      if (!out.includes(value)) out.push(value);
    }
  }
  return out;
}

function splitTopLevelPlus(expr: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let start = 0;
  let quote: '"' | "'" | '`' | null = null;
  for (let i = 0; i < expr.length; i++) {
    const ch = expr[i]!;
    const prev = expr[i - 1];
    if (quote) {
      if (ch === quote && prev !== '\\') quote = null;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === '`') {
      quote = ch;
      continue;
    }
    if (ch === '(' || ch === '[' || ch === '{') {
      depth++;
      continue;
    }
    if (ch === ')' || ch === ']' || ch === '}') {
      depth = Math.max(0, depth - 1);
      continue;
    }
    if (ch === '+' && depth === 0) {
      parts.push(expr.slice(start, i).trim());
      start = i + 1;
    }
  }
  parts.push(expr.slice(start).trim());
  return parts;
}

function stripWrappingParens(value: string): string {
  let s = value.trim();
  while (s.startsWith('(') && s.endsWith(')')) {
    const inner = s.slice(1, -1).trim();
    if (!inner) break;
    s = inner;
  }
  return s;
}

function inferHttpMethod(methodName: string): string | null {
  if (methodName === 'post') return 'POST';
  return DIRECT_METHOD_TO_HTTP[methodName] ?? null;
}

function inferExchangeHttpMethod(arg: string | undefined): string | null {
  if (!arg) return null;
  const match = /\bHttpMethod\.([A-Z]+)\b/.exec(arg);
  return match?.[1] ?? null;
}

function firstStringLiteral(expr: string): string | null {
  const m = /"([^"]+)"|'([^']+)'/.exec(expr);
  return m ? (m[1] ?? m[2] ?? null) : null;
}

function stripJavaStringLiteral(value: string): string {
  return value.replace(/^["']|["']$/g, '');
}

function mergeBinding(bindings: UrlBindings, name: string, urls: readonly string[]): void {
  const existing = bindings.get(name) ?? [];
  for (const url of urls) {
    if (!existing.includes(url)) existing.push(url);
  }
  bindings.set(name, existing);
}

function splitTopLevelArgs(content: string, openParenIndex: number): string[] {
  const args: string[] = [];
  let depth = 0;
  let start = openParenIndex + 1;
  let quote: '"' | "'" | '`' | null = null;
  for (let i = openParenIndex + 1; i < content.length; i++) {
    const ch = content[i]!;
    const prev = content[i - 1];
    if (quote) {
      if (ch === quote && prev !== '\\') quote = null;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === '`') {
      quote = ch;
      continue;
    }
    if (ch === '(') {
      depth++;
      continue;
    }
    if (ch === ')') {
      if (depth === 0) {
        args.push(content.slice(start, i).trim());
        return args;
      }
      depth--;
      continue;
    }
    if (ch === ',' && depth === 0) {
      args.push(content.slice(start, i).trim());
      start = i + 1;
    }
  }
  return args;
}

function lineForIndex(content: string, index: number): number {
  let lines = 0;
  for (let i = 0; i < index; i++) {
    if (content.charCodeAt(i) === 10) lines++;
  }
  return lines;
}
