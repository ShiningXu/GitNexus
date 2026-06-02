import { describe, expect, it } from 'vitest';
import {
  extractJavaHttpConsumerCalls,
  normalizeJavaHttpConsumerRoute,
} from '../../src/core/ingestion/route-extractors/java-http-consumers.js';

describe('Java HTTP consumer extraction', () => {
  it('normalizes absolute Java client URLs to route paths', () => {
    expect(normalizeJavaHttpConsumerRoute('http://svc.internal/api/items?x=1')).toBe('/api/items');
    expect(normalizeJavaHttpConsumerRoute('/api/items/${id}')).toBe('/api/items/{param}');
  });

  it('extracts RestTemplate calls through @Value defaults and UriComponentsBuilder', () => {
    const source = `
      class RecommendApi {
        @Value("\${recommend.url:http://ai.vip.qiyi.domain/rc/sendMaterial}")
        private String dataSyncUrl;
        private RestTemplate restTemplate;

        void syncData() {
          UriComponentsBuilder builder = UriComponentsBuilder.fromUriString(dataSyncUrl);
          URI uri = builder.build().toUri();
          restTemplate.exchange(uri, HttpMethod.POST, null, String.class);
        }
      }
    `;

    expect(extractJavaHttpConsumerCalls('src/RecommendApi.java', source)).toEqual([
      expect.objectContaining({
        rawUrl: 'http://ai.vip.qiyi.domain/rc/sendMaterial',
        routePath: '/rc/sendMaterial',
        httpMethod: 'POST',
      }),
    ]);
  });

  it('extracts ternary URL variables and direct RestTemplate methods', () => {
    const source = `
      class FlowApi {
        @Value("\${flow.add.url:http://vip-operation-flow-online/flow-api/flow/addFlowWithData}")
        private String flowAddUrl;
        @Value("\${flow.qsm.add.url:http://vip-operation-flow-api-test/flow-api/flow/addFlowWithData}")
        private String flowQsmAddUrl;
        private RestTemplate restTemplate;

        void addFlowWithData() {
          String url = qsmValid == 1 ? flowQsmAddUrl : flowAddUrl;
          restTemplate.postForObject(url, reqDTO, CommonResult.class);
        }
      }
    `;

    const calls = extractJavaHttpConsumerCalls('src/FlowApi.java', source);
    expect(calls.map((c) => `${c.httpMethod} ${c.routePath}`).sort()).toEqual([
      'POST /flow-api/flow/addFlowWithData',
      'POST /flow-api/flow/addFlowWithData',
    ]);
    expect(new Set(calls.map((c) => c.rawUrl)).size).toBe(2);
  });

  it('extracts custom FastHttpClient-style wrappers when the first argument resolves to a URL', () => {
    const source = `
      class ActivityCheckService {
        @Value("\${api.baseline.viewact:http://cloud-package.online.qiyi.qae/cloud_package/api/v1/ai/act}")
        private String apiBaselineViewActUrl;
        private FastHttpClient fastHttpClient;

        void check() {
          fastHttpClient.getForResponseEntity(apiBaselineViewActUrl, reqMap);
        }
      }
    `;

    expect(extractJavaHttpConsumerCalls('src/ActivityCheckService.java', source)).toEqual([
      expect.objectContaining({
        routePath: '/cloud_package/api/v1/ai/act',
        httpMethod: 'GET',
      }),
    ]);
  });

  it('combines bound base URLs with path literals before normalizing routes', () => {
    const source = `
      class VccApi {
        @Value("\${vcc.vip-commodity.eureka.host:http://VIP-COMMODITY-CENTER-ONLINE/vip-commodity}")
        private String vccEurekaHost;
        @Value("\${vcc.vip-commodity.eureka.host.qsm:http://vip-commodity-center.qsm.qiyi.middle/vip-commodity}")
        private String vccEurekaHostQsm;

        void batchQuerySku() {
          String baseUrl = qsmUtils.getQsmSwitch() ? vccEurekaHostQsm : vccEurekaHost;
          String url = baseUrl + "/basicAndSku/sku/batchQuery";
          restTemplateBase.postForObject(url, request, HttpClientResponseDTO.class);
        }
      }
    `;

    const calls = extractJavaHttpConsumerCalls('src/VccApi.java', source);
    expect(calls.map((c) => c.routePath).sort()).toEqual([
      '/vip-commodity/basicAndSku/sku/batchQuery',
      '/vip-commodity/basicAndSku/sku/batchQuery',
    ]);
    expect(calls.every((c) => c.httpMethod === 'POST')).toBe(true);
  });

  it('combines a resolved URL variable with an inline path literal argument', () => {
    const source = `
      class VccAdminApi {
        @Value("\${vcc.admin.api.url:http://VIP-COMMODITY-CENTER-ADMIN-TEST/vip-commodity-admin}")
        private String vccAdminApiUrl;
        @Value("\${vcc.admin.api.qsm.url:http://vip-commodity-center-admin.qsm.qiyi.middle/vip-commodity-admin}")
        private String vccAdminApiQsmUrl;

        void generaterSkuId() {
          RestTemplate restTemplate = qsmUtils.getQsmSwitch() ? this.longRestTemplate : this.lbLongRestTemplate;
          String url = qsmUtils.getQsmSwitch() ? this.vccAdminApiQsmUrl : this.vccAdminApiUrl;
          restTemplate.postForObject(url + "/sku/generateSkuId", entity, HttpClientResponseDTO.class);
        }
      }
    `;

    const calls = extractJavaHttpConsumerCalls('src/VccAdminApi.java', source);
    expect(calls.map((c) => c.routePath).sort()).toEqual([
      '/vip-commodity-admin/sku/generateSkuId',
      '/vip-commodity-admin/sku/generateSkuId',
    ]);
    expect(calls.every((c) => c.httpMethod === 'POST')).toBe(true);
  });
});
