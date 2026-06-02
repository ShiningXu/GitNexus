import { generateId } from '../../../lib/utils.js';
import type { KnowledgeGraph } from '../../graph/types.js';

export interface JavaRocketMqConfigFile {
  filePath: string;
  content: string;
}

export interface JavaRocketMqConsumerEdge {
  filePath: string;
  role: 'consumer' | 'producer';
  topicName: string;
  rawTopic: string;
  consumerGroup?: string;
  tag?: string;
  lineNumber: number;
  framework: string;
}

type StringBindings = Map<string, string[]>;
type RocketMqBeanProperties = Map<string, Map<string, string[]>>;

const JAVA_IDENTIFIER_RE = /^[A-Za-z_$][\w$]*$/;
const PLACEHOLDER_RE = /^\$\{([^}:]+)(?::([^}]*))?\}$/;
const CONFIG_FILE_RE = /\.(?:properties|ya?ml)$/i;

export function extractJavaRocketMqConsumerEdges(
  filePath: string,
  content: string,
  config: ReadonlyMap<string, readonly string[]> = new Map(),
  beanProperties: ReadonlyMap<string, ReadonlyMap<string, readonly string[]>> = new Map(),
): JavaRocketMqConsumerEdge[] {
  const bindings = collectJavaStringBindings(content, config);
  const localBeanProperties = collectRocketMqPropertyBeans(content, bindings);
  const availableBeanProperties = mergeBeanProperties(localBeanProperties, beanProperties);
  const edges: JavaRocketMqConsumerEdge[] = [];
  const seen = new Set<string>();

  const emit = (edge: JavaRocketMqConsumerEdge) => {
    const key = `${edge.filePath}:${edge.lineNumber}:${edge.framework}:${edge.topicName}:${edge.consumerGroup ?? ''}:${edge.tag ?? ''}`;
    if (seen.has(key)) return;
    seen.add(key);
    edges.push(edge);
  };

  for (const match of content.matchAll(/@RocketMQMessageListener\s*\(/g)) {
    const openParen = match.index + match[0].length - 1;
    const body = readBalanced(content, openParen);
    if (!body) continue;
    const attrs = parseAnnotationAttributes(body);
    const topics = resolveJavaStringExpression(attrs.get('topic') ?? '', bindings, config);
    if (topics.length === 0) continue;
    const groups = resolveJavaStringExpression(attrs.get('consumerGroup') ?? '', bindings, config);
    const tags = resolveJavaStringExpression(
      attrs.get('selectorExpression') ?? attrs.get('tags') ?? '',
      bindings,
      config,
    );
    for (const topic of topics) {
      emit({
        filePath,
        role: 'consumer',
        topicName: topic.value,
        rawTopic: topic.raw,
        consumerGroup: firstValue(groups),
        tag: firstValue(tags),
        lineNumber: lineForIndex(content, match.index) + 1,
        framework: 'rocketmq-spring-listener',
      });
    }
  }

  for (const match of content.matchAll(/\.subscribe\s*\(/g)) {
    const openParen = match.index + match[0].length - 1;
    const args = splitTopLevelArgs(content, openParen);
    if (args.length === 0) continue;
    const topics = resolveJavaStringExpression(
      args[0] ?? '',
      bindings,
      config,
      availableBeanProperties,
    );
    if (topics.length === 0) continue;
    const tags = resolveJavaStringExpression(
      args[1] ?? '',
      bindings,
      config,
      availableBeanProperties,
    );
    const group = inferNearbyConsumerGroup(
      content,
      match.index,
      bindings,
      config,
      availableBeanProperties,
    );
    for (const topic of topics) {
      emit({
        filePath,
        role: 'consumer',
        topicName: topic.value,
        rawTopic: topic.raw,
        consumerGroup: group,
        tag: firstValue(tags),
        lineNumber: lineForIndex(content, match.index) + 1,
        framework: 'rocketmq-client-subscribe',
      });
    }
  }

  for (const match of content.matchAll(/\bhandleConsumer\s*\(\s*([A-Za-z_$][\w$]*)\s*,/g)) {
    const beanName = match[1];
    const bean = beanName ? availableBeanProperties.get(beanName) : undefined;
    const topics = bean?.get('Topic') ?? [];
    if (!beanName || topics.length === 0) continue;
    const group = bean?.get('GroupName')?.[0];
    const tag = bean?.get('Tag')?.[0];
    for (const topicName of topics) {
      emit({
        filePath,
        role: 'consumer',
        topicName,
        rawTopic: beanName,
        consumerGroup: group,
        tag,
        lineNumber: lineForIndex(content, match.index) + 1,
        framework: 'rocketmq-client-factory',
      });
    }
  }

  for (const match of content.matchAll(
    /\brocketMQTemplate\.(syncSend|asyncSend|sendOneWay|syncSendInDelaySeconds|syncSendDelayTimeSeconds|sendMessageInTransaction)\s*\(/g,
  )) {
    const methodName = match[1] ?? 'send';
    const openParen = match.index + match[0].length - 1;
    const args = splitTopLevelArgs(content, openParen);
    if (args.length === 0) continue;
    const topics = resolveJavaStringExpression(
      args[0] ?? '',
      bindings,
      config,
      availableBeanProperties,
    );
    for (const topic of topics) {
      emit({
        filePath,
        role: 'producer',
        topicName: topic.value,
        rawTopic: topic.raw,
        lineNumber: lineForIndex(content, match.index) + 1,
        framework: `rocketmq-template-${methodName}`,
      });
    }
  }

  const rocketMessageVars = collectRocketMessageVariables(content);
  for (const match of content.matchAll(/\b([A-Za-z_$][\w$]*)\.setTopic\s*\(/g)) {
    const messageVar = match[1];
    if (!messageVar || !rocketMessageVars.has(messageVar)) continue;
    const openParen = match.index + match[0].length - 1;
    const args = splitTopLevelArgs(content, openParen);
    const topics = resolveJavaStringExpression(
      args[0] ?? '',
      bindings,
      config,
      availableBeanProperties,
    );
    const tag = inferRocketMessageTag(
      content,
      messageVar,
      match.index,
      bindings,
      config,
      availableBeanProperties,
    );
    for (const topic of topics) {
      emit({
        filePath,
        role: 'producer',
        topicName: topic.value,
        rawTopic: topic.raw,
        tag,
        lineNumber: lineForIndex(content, match.index) + 1,
        framework: 'rocketmessage-set-topic',
      });
    }
  }

  for (const match of content.matchAll(/\bnew\s+Message\s*\(/g)) {
    const openParen = match.index + match[0].length - 1;
    const args = splitTopLevelArgs(content, openParen);
    if (args.length === 0) continue;
    const topics = resolveJavaStringExpression(
      args[0] ?? '',
      bindings,
      config,
      availableBeanProperties,
    );
    if (topics.length === 0) continue;
    const tags = resolveJavaStringExpression(
      args[1] ?? '',
      bindings,
      config,
      availableBeanProperties,
    );
    for (const topic of topics) {
      emit({
        filePath,
        role: 'producer',
        topicName: topic.value,
        rawTopic: topic.raw,
        tag: firstValue(tags),
        lineNumber: lineForIndex(content, match.index) + 1,
        framework: 'rocketmq-message-constructor',
      });
    }
  }

  return edges;
}

export function extractJavaRocketMqConfig(
  files: readonly JavaRocketMqConfigFile[],
): Map<string, string[]> {
  const config = new Map<string, string[]>();
  for (const file of files) {
    if (!CONFIG_FILE_RE.test(file.filePath)) continue;
    const entries = file.filePath.endsWith('.properties')
      ? parsePropertiesConfig(file.content)
      : parseYamlConfig(file.content);
    for (const [key, value] of entries) mergeBinding(config, key, [value]);
  }
  return config;
}

export function extractJavaRocketMqBeanProperties(
  files: readonly JavaRocketMqConfigFile[],
  config: ReadonlyMap<string, readonly string[]> = new Map(),
): RocketMqBeanProperties {
  const out: RocketMqBeanProperties = new Map();
  for (const file of files) {
    if (!file.filePath.endsWith('.java')) continue;
    const bindings = collectJavaStringBindings(file.content, config);
    for (const [beanName, props] of collectRocketMqPropertyBeans(file.content, bindings)) {
      out.set(beanName, props);
    }
  }
  return out;
}

export function processJavaRocketMqConsumerEdges(
  graph: KnowledgeGraph,
  edges: readonly JavaRocketMqConsumerEdge[],
): void {
  for (const edge of edges) {
    const sourceId = generateId('File', edge.filePath);
    const topicNodeId = generateId('CodeElement', `rocketmq-topic:${edge.topicName}`);
    if (!graph.getNode(topicNodeId)) {
      graph.addNode({
        id: topicNodeId,
        label: 'CodeElement',
        properties: {
          name: edge.topicName,
          filePath: edge.filePath,
          startLine: edge.lineNumber,
          endLine: edge.lineNumber,
          content: edge.topicName,
          description: 'RocketMQ topic',
          kind: 'RocketMQTopic',
          broker: 'rocketmq',
        },
      });
    }

    graph.addRelationship({
      id: generateId(
        edge.role === 'producer' ? 'PRODUCES_TOPIC' : 'CONSUMES_TOPIC',
        `${sourceId}->${topicNodeId}:${edge.lineNumber}:${edge.consumerGroup ?? ''}:${edge.tag ?? ''}`,
      ),
      sourceId,
      targetId: topicNodeId,
      type: edge.role === 'producer' ? 'PRODUCES_TOPIC' : 'CONSUMES_TOPIC',
      confidence: edge.rawTopic.startsWith('${') ? 0.75 : 0.85,
      reason: [
        `java-rocketmq-${edge.role}`,
        `framework:${edge.framework}`,
        `topic:${edge.topicName}`,
        edge.consumerGroup ? `group:${edge.consumerGroup}` : null,
        edge.tag ? `tag:${edge.tag}` : null,
        `rawTopic:${edge.rawTopic}`,
      ]
        .filter(Boolean)
        .join('|'),
    });
  }
}

interface ResolvedString {
  value: string;
  raw: string;
}

function collectJavaStringBindings(
  content: string,
  config: ReadonlyMap<string, readonly string[]>,
): StringBindings {
  const bindings: StringBindings = new Map();

  const valueFieldRe =
    /@Value\s*\(\s*"([^"]+)"\s*\)\s*(?:\r?\n\s*(?:@\w+(?:\([^)]*\))?\s*)?)*\s*(?:private|protected|public)?\s*(?:static\s+)?(?:final\s+)?String\s+([A-Za-z_$][\w$]*)/g;
  for (const match of content.matchAll(valueFieldRe)) {
    const values = resolvePlaceholder(match[1] ?? '', config);
    if (match[2] && values.length > 0)
      mergeBinding(
        bindings,
        match[2],
        values.map((v) => v.value),
      );
  }

  const literalFieldRe =
    /\b(?:private|protected|public)?\s*(?:static\s+)?(?:final\s+)?String\s+([A-Za-z_$][\w$]*)\s*=\s*([^;\n]+);/g;
  for (const match of content.matchAll(literalFieldRe)) {
    const name = match[1];
    const expr = match[2];
    if (!name || !expr || bindings.has(name)) continue;
    const values = resolveJavaStringExpression(expr, bindings, config);
    if (values.length > 0)
      mergeBinding(
        bindings,
        name,
        values.map((v) => v.value),
      );
  }

  let changed = true;
  for (let pass = 0; pass < 4 && changed; pass++) {
    changed = false;
    const assignRe = /\b(?:String|var)\s+([A-Za-z_$][\w$]*)\s*=\s*([^;\n]+);/g;
    for (const match of content.matchAll(assignRe)) {
      const name = match[1];
      const expr = match[2];
      if (!name || !expr || bindings.has(name)) continue;
      const values = resolveJavaStringExpression(expr, bindings, config);
      if (values.length > 0) {
        mergeBinding(
          bindings,
          name,
          values.map((v) => v.value),
        );
        changed = true;
      }
    }
  }

  return bindings;
}

function collectRocketMqPropertyBeans(
  content: string,
  bindings: ReadonlyMap<string, readonly string[]>,
): RocketMqBeanProperties {
  const beans: RocketMqBeanProperties = new Map();
  const methodRe =
    /@Bean(?:\s*\([^)]*\))?\s*(?:public|private|protected)?\s*RocketMQProperty\s+([A-Za-z_$][\w$]*)\s*\([^)]*\)\s*\{/g;
  for (const match of content.matchAll(methodRe)) {
    const beanName = match[1];
    if (!beanName) continue;
    const openBrace = content.indexOf('{', match.index);
    const body = readBalanced(content, openBrace);
    if (!body) continue;
    const props = new Map<string, string[]>();
    for (const setter of body.matchAll(/\.set(Topic|GroupName|Tag)\s*\(([^)]*)\)/g)) {
      const prop = setter[1] === 'GroupName' ? 'GroupName' : setter[1];
      const values = resolveJavaStringExpression(setter[2] ?? '', bindings);
      if (values.length > 0)
        props.set(
          prop,
          values.map((v) => v.value),
        );
    }
    if (props.size > 0) beans.set(beanName, props);
  }
  return beans;
}

function collectRocketMessageVariables(content: string): Set<string> {
  const vars = new Set<string>();
  for (const match of content.matchAll(
    /\bRocketMessage(?:\s*<[^>]+>)?\s+([A-Za-z_$][\w$]*)\s*=/g,
  )) {
    if (match[1]) vars.add(match[1]);
  }
  return vars;
}

function inferRocketMessageTag(
  content: string,
  messageVar: string,
  topicSetIndex: number,
  bindings: ReadonlyMap<string, readonly string[]>,
  config: ReadonlyMap<string, readonly string[]>,
  beanProperties: ReadonlyMap<string, ReadonlyMap<string, readonly string[]>>,
): string | undefined {
  const after = content.slice(topicSetIndex, Math.min(content.length, topicSetIndex + 800));
  const pattern = new RegExp(`\\b${escapeRegex(messageVar)}\\.setTags\\s*\\(([^)]*)\\)`);
  const match = pattern.exec(after);
  if (!match) return undefined;
  return firstValue(resolveJavaStringExpression(match[1] ?? '', bindings, config, beanProperties));
}

function mergeBeanProperties(
  local: RocketMqBeanProperties,
  project: ReadonlyMap<string, ReadonlyMap<string, readonly string[]>>,
): ReadonlyMap<string, ReadonlyMap<string, readonly string[]>> {
  if (project.size === 0) return local;
  const merged: RocketMqBeanProperties = new Map();
  for (const [name, props] of project) merged.set(name, copyBeanProps(props));
  for (const [name, props] of local) merged.set(name, copyBeanProps(props));
  return merged;
}

function copyBeanProps(props: ReadonlyMap<string, readonly string[]>): Map<string, string[]> {
  const out = new Map<string, string[]>();
  for (const [key, values] of props) out.set(key, [...values]);
  return out;
}

function inferNearbyConsumerGroup(
  content: string,
  subscribeIndex: number,
  bindings: ReadonlyMap<string, readonly string[]>,
  config: ReadonlyMap<string, readonly string[]>,
  beanProperties: ReadonlyMap<string, ReadonlyMap<string, readonly string[]>>,
): string | undefined {
  const window = content.slice(Math.max(0, subscribeIndex - 1200), subscribeIndex);
  const matches = [...window.matchAll(/new\s+DefaultMQPushConsumer\s*\(([^)]*)\)/g)];
  const expr = matches.at(-1)?.[1];
  if (!expr) return undefined;
  return firstValue(resolveJavaStringExpression(expr, bindings, config, beanProperties));
}

function resolveJavaStringExpression(
  expr: string,
  bindings: ReadonlyMap<string, readonly string[]>,
  config: ReadonlyMap<string, readonly string[]> = new Map(),
  beanProperties: ReadonlyMap<string, ReadonlyMap<string, readonly string[]>> = new Map(),
): ResolvedString[] {
  const out: ResolvedString[] = [];
  const add = (value: string, raw = value) => {
    const trimmed = value.trim();
    if (!isTopicLikeValue(trimmed)) return;
    if (!out.some((v) => v.value === trimmed && v.raw === raw)) out.push({ value: trimmed, raw });
  };

  const normalized = expr.trim();
  if (!normalized) return out;

  for (const value of resolvePlaceholder(stripJavaStringLiteral(normalized), config)) {
    add(value.value, value.raw);
  }

  for (const literal of allStringLiterals(normalized)) add(literal);

  if (JAVA_IDENTIFIER_RE.test(normalized)) {
    for (const value of bindings.get(normalized) ?? []) add(value);
  }

  for (const id of normalized.match(/\b[A-Za-z_$][\w$]*\b/g) ?? []) {
    for (const value of bindings.get(id) ?? []) add(value);
  }

  for (const getter of normalized.matchAll(
    /\b([A-Za-z_$][\w$]*)\.get(Topic|GroupName|Tag)\s*\(\s*\)/g,
  )) {
    const bean = beanProperties.get(getter[1] ?? '');
    const values = bean?.get(getter[2] ?? '') ?? [];
    for (const value of values) add(value);
  }

  return out;
}

function resolvePlaceholder(
  valueSpec: string,
  config: ReadonlyMap<string, readonly string[]>,
): ResolvedString[] {
  const spec = valueSpec.trim();
  const match = PLACEHOLDER_RE.exec(spec);
  if (!match) return [];
  const key = match[1] ?? '';
  const defaults = match[2] !== undefined && match[2] !== '' ? [match[2]] : [];
  const configured = config.get(key) ?? [];
  const candidates = configured.length > 0 ? configured : defaults;
  return candidates
    .map((value) => value.trim())
    .filter(isTopicLikeValue)
    .map((value) => ({ value, raw: spec }));
}

function parseAnnotationAttributes(body: string): Map<string, string> {
  const attrs = new Map<string, string>();
  for (const part of splitTopLevelList(body)) {
    const idx = part.indexOf('=');
    if (idx <= 0) continue;
    attrs.set(part.slice(0, idx).trim(), part.slice(idx + 1).trim());
  }
  return attrs;
}

function parsePropertiesConfig(content: string): Map<string, string> {
  const out = new Map<string, string>();
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#') || trimmed.startsWith('!')) continue;
    const match = /^([^:=\s]+)\s*[:=]\s*(.*)$/.exec(trimmed);
    if (!match) continue;
    out.set(match[1]!.trim(), stripInlineComment(match[2]!.trim()));
  }
  return out;
}

function parseYamlConfig(content: string): Map<string, string> {
  const out = new Map<string, string>();
  const stack: { indent: number; key: string }[] = [];
  for (const rawLine of content.split(/\r?\n/)) {
    const withoutComment = rawLine.replace(/\s+#.*$/, '');
    if (!withoutComment.trim() || withoutComment.trimStart().startsWith('#')) continue;
    const match = /^(\s*)([A-Za-z0-9_.-]+)\s*:\s*(.*)$/.exec(withoutComment);
    if (!match) continue;
    const indent = match[1]!.length;
    const key = match[2]!;
    const rawValue = match[3]!.trim();
    while (stack.length > 0 && stack[stack.length - 1]!.indent >= indent) stack.pop();
    const fullKey = [...stack.map((s) => s.key), key].join('.');
    if (rawValue === '') {
      stack.push({ indent, key });
    } else {
      out.set(fullKey, stripJavaStringLiteral(rawValue));
    }
  }
  return out;
}

function splitTopLevelArgs(content: string, openParenIndex: number): string[] {
  const body = readBalanced(content, openParenIndex);
  return body ? splitTopLevelList(body) : [];
}

function splitTopLevelList(body: string): string[] {
  const args: string[] = [];
  let current = '';
  let depth = 0;
  let quote: string | null = null;

  for (let i = 0; i < body.length; i++) {
    const ch = body[i]!;
    const prev = body[i - 1];
    if (quote) {
      current += ch;
      if (ch === quote && prev !== '\\') quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      current += ch;
      continue;
    }
    if (ch === '(' || ch === '[' || ch === '{') depth++;
    if (ch === ')' || ch === ']' || ch === '}') depth--;
    if (ch === ',' && depth === 0) {
      args.push(current.trim());
      current = '';
      continue;
    }
    current += ch;
  }

  if (current.trim()) args.push(current.trim());
  return args;
}

function readBalanced(content: string, openIndex: number): string | null {
  const open = content[openIndex];
  const close = open === '(' ? ')' : open === '{' ? '}' : null;
  if (!close) return null;
  let depth = 0;
  let quote: string | null = null;
  for (let i = openIndex; i < content.length; i++) {
    const ch = content[i]!;
    const prev = content[i - 1];
    if (quote) {
      if (ch === quote && prev !== '\\') quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      continue;
    }
    if (ch === open) depth++;
    if (ch === close) depth--;
    if (depth === 0) return content.slice(openIndex + 1, i);
  }
  return null;
}

function allStringLiterals(expr: string): string[] {
  return [...expr.matchAll(/"([^"\\]*(?:\\.[^"\\]*)*)"|'([^'\\]*(?:\\.[^'\\]*)*)'/g)].map((m) =>
    unescapeJavaString(m[1] ?? m[2] ?? ''),
  );
}

function unescapeJavaString(value: string): string {
  return value.replace(/\\"/g, '"').replace(/\\'/g, "'").replace(/\\\\/g, '\\');
}

function stripJavaStringLiteral(value: string): string {
  return value.replace(/^["']|["']$/g, '');
}

function stripInlineComment(value: string): string {
  const hash = value.search(/\s+#/);
  return hash >= 0 ? value.slice(0, hash).trim() : value;
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function isTopicLikeValue(value: string): boolean {
  if (!value || value === '*' || value.startsWith('${')) return false;
  if (/^https?:\/\//i.test(value)) return false;
  if (value.includes(':9876') || value.includes(';')) return false;
  return /[A-Za-z0-9_-]/.test(value);
}

function firstValue(values: readonly ResolvedString[]): string | undefined {
  return values[0]?.value;
}

function mergeBinding(
  bindings: Map<string, string[]>,
  name: string,
  values: readonly string[],
): void {
  const existing = bindings.get(name) ?? [];
  for (const value of values) {
    const trimmed = value.trim();
    if (trimmed && !existing.includes(trimmed)) existing.push(trimmed);
  }
  bindings.set(name, existing);
}

function lineForIndex(content: string, index: number): number {
  let line = 0;
  for (let i = 0; i < index; i++) {
    if (content.charCodeAt(i) === 10) line++;
  }
  return line;
}
