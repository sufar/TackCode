import type { ZCodeBuiltinRelease } from "@zcode/provider-node";
import type { ApiClient } from "@zcode/shared";

interface FetchZCodeBuiltinRemoteReleaseOptions {
  readonly apiClient: ApiClient;
  readonly endpointOrigin: string;
  readonly appVersion: string;
  readonly platform: string;
  readonly signal?: AbortSignal;
}

/** Services 仅注入既有网络装配；URL、预算与 Release 校验由 provider-node 唯一实现。 */
export async function fetchZCodeBuiltinRemoteRelease(
  options: FetchZCodeBuiltinRemoteReleaseOptions,
): Promise<ZCodeBuiltinRelease | null> {
  // TackCode: never download the remote builtin provider catalog — the
  // upstream catalog is Zhipu-curated and the fork ships its own bundled
  // config. (Upstream would fetch /api/v1/client/configs on the endpoint.)
  void options;
  return null;
}
