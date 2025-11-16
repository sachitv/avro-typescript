import type {
  FetchTransportOptions,
  StatelessTransportFactory,
} from "./transport_helpers.ts";

export function createFetchTransport(
  endpoint: string | URL,
  options: FetchTransportOptions = {},
): StatelessTransportFactory {
  const url = typeof endpoint === "string" ? endpoint : endpoint.toString();
  return async () => {
    const stream = new TransformStream<Uint8Array>();
    const fetchFn = options.fetch ?? fetch;
    const init: RequestInit = { ...(options.init ?? {}) };
    const headers = new Headers(init.headers ?? options.headers);
    if (!headers.has("content-type")) {
      headers.set("content-type", "avro/binary");
    }
    init.method = options.method ?? init.method ?? "POST";
    init.headers = headers;
    init.body = stream.readable;

    const response = await fetchFn(url, init);
    if (!response.body) {
      throw new Error("Fetch response has no body.");
    }
    return {
      readable: response.body,
      writable: stream.writable,
    };
  };
}
