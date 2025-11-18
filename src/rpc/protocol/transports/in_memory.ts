import type {
  BinaryDuplexLike,
  BinaryWritable,
  StatelessTransportFactory,
} from "./transport_helpers.ts";

export interface InMemoryTransportPair {
  /**
   * Factory passed to emitters to establish a new client connection.
   */
  clientFactory: StatelessTransportFactory;
  /**
   * Accepts the next server-side connection created by the client factory.
   */
  accept(): Promise<BinaryDuplexLike>;
}

interface DuplexPair {
  client: BinaryDuplexLike;
  server: BinaryDuplexLike;
}

function createInMemoryTransportPair(): InMemoryTransportPair {
  const pendingServers: BinaryDuplexLike[] = [];
  const pendingAccepts: Array<(duplex: BinaryDuplexLike) => void> = [];

  const clientFactory: StatelessTransportFactory = () => {
    const pair = createDuplexPair();
    const resolver = pendingAccepts.shift();
    if (resolver) {
      resolver(pair.server);
    } else {
      pendingServers.push(pair.server);
    }
    return Promise.resolve(pair.client);
  };

  const accept = (): Promise<BinaryDuplexLike> => {
    const queued = pendingServers.shift();
    if (queued) {
      return Promise.resolve(queued);
    }
    return new Promise((resolve) => {
      pendingAccepts.push(resolve);
    });
  };

  return { clientFactory, accept };
}

export function createInMemoryTransport(): StatelessTransportFactory {
  return createInMemoryTransportPair().clientFactory;
}

export { createInMemoryTransportPair };

function createDuplexPair(): DuplexPair {
  const clientChannel = new MemoryChannel();
  const serverChannel = new MemoryChannel();

  const client: BinaryDuplexLike = {
    readable: clientChannel,
    writable: new MemoryChannelWritable(serverChannel),
  };

  const server: BinaryDuplexLike = {
    readable: serverChannel,
    writable: new MemoryChannelWritable(clientChannel),
  };

  return { client, server };
}

class MemoryChannel {
  private closed = false;
  private pendingResolvers: Array<(value: Uint8Array | null) => void> = [];
  private queue: Uint8Array[] = [];

  read(): Promise<Uint8Array | null> {
    if (this.queue.length > 0) {
      const chunk = this.queue.shift()!;
      return Promise.resolve(chunk);
    }
    if (this.closed) {
      return Promise.resolve(null);
    }
    return new Promise<Uint8Array | null>((resolve) => {
      this.pendingResolvers.push(resolve);
    });
  }

  enqueue(chunk: Uint8Array) {
    if (this.closed) {
      throw new Error("Cannot write to closed channel.");
    }
    if (this.pendingResolvers.length > 0) {
      const resolver = this.pendingResolvers.shift()!;
      resolver(chunk);
    } else {
      this.queue.push(chunk);
    }
  }

  close(): Promise<void> {
    if (this.closed) {
      return Promise.resolve();
    }
    this.closed = true;
    while (this.pendingResolvers.length > 0) {
      const resolver = this.pendingResolvers.shift()!;
      resolver(null);
    }
    return Promise.resolve();
  }
}

class MemoryChannelWritable implements BinaryWritable {
  constructor(private readonly target: MemoryChannel) {}

  write(chunk: Uint8Array) {
    this.target.enqueue(chunk);
    return Promise.resolve();
  }

  close() {
    return this.target.close();
  }
}
