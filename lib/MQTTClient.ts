import mqtt, { MqttClient, IClientOptions } from 'mqtt';

export type MessageHandler = (topic: string, payload: string) => void;
export type BinaryMessageHandler = (topic: string, payload: Buffer) => void;

// ---------- Per-device MQTT client ----------

export class FrigateMQTTClient {

  private client: MqttClient | null = null;
  private readonly subscriptions: Map<string, Set<MessageHandler>> = new Map();
  private readonly binarySubscriptions: Map<string, Set<BinaryMessageHandler>> = new Map();
  private reconnectDelay: number = 1000;
  private destroyed: boolean = false;

  private onConnectCb?: () => void;
  private onDisconnectCb?: () => void;

  constructor(
    private readonly brokerUrl: string,
    private readonly username?: string,
    private readonly password?: string,
  ) {}

  connect(onConnect?: () => void, onDisconnect?: () => void): void {
    this.onConnectCb = onConnect;
    this.onDisconnectCb = onDisconnect;
    this.doConnect();
  }

  private doConnect(): void {
    if (this.destroyed) return;

    const options: IClientOptions = {
      clientId: `homey-frigate-${Math.random().toString(16).slice(2, 10)}`,
      clean: true,
      reconnectPeriod: 0, // we manage reconnect ourselves for backoff control
    };
    if (this.username) options.username = this.username;
    if (this.password) options.password = this.password;

    this.client = mqtt.connect(this.brokerUrl, options);

    this.client.on('connect', () => {
      this.reconnectDelay = 1000;
      for (const topic of this.subscriptions.keys()) this.client!.subscribe(topic);
      for (const topic of this.binarySubscriptions.keys()) this.client!.subscribe(topic);
      this.onConnectCb?.();
    });

    this.client.on('message', (topic: string, payload: Buffer) => {
      // String handlers — for JSON/text topics
      const msg = payload.toString();
      for (const [pattern, handlers] of this.subscriptions.entries()) {
        if (this.matches(pattern, topic)) handlers.forEach((h) => h(topic, msg));
      }
      // Binary handlers — for JPEG snapshot topics
      for (const [pattern, handlers] of this.binarySubscriptions.entries()) {
        if (this.matches(pattern, topic)) handlers.forEach((h) => h(topic, payload));
      }
    });

    this.client.on('error', () => {
      // Handled via 'close' to avoid unhandled EventEmitter error
    });

    this.client.on('close', () => {
      if (this.destroyed) return;
      this.client = null;
      this.onDisconnectCb?.();
      this.scheduleReconnect();
    });
  }

  private scheduleReconnect(): void {
    if (this.destroyed) return;
    setTimeout(() => this.doConnect(), this.reconnectDelay);
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
  }

  subscribe(topic: string, handler: MessageHandler): void {
    if (!this.subscriptions.has(topic)) {
      this.subscriptions.set(topic, new Set());
      this.client?.subscribe(topic);
    }
    this.subscriptions.get(topic)!.add(handler);
  }

  subscribeBinary(topic: string, handler: BinaryMessageHandler): void {
    if (!this.binarySubscriptions.has(topic)) {
      this.binarySubscriptions.set(topic, new Set());
      this.client?.subscribe(topic);
    }
    this.binarySubscriptions.get(topic)!.add(handler);
  }

  publish(topic: string, payload: string = ''): void {
    this.client?.publish(topic, payload);
  }

  disconnect(): void {
    this.destroyed = true;
    this.client?.end(true);
    this.client = null;
  }

  // MQTT wildcard matching: + = one level, # = rest of path
  private matches(pattern: string, topic: string): boolean {
    const pp = pattern.split('/');
    const tp = topic.split('/');
    for (let i = 0; i < pp.length; i++) {
      if (pp[i] === '#') return true;
      if (pp[i] !== '+' && pp[i] !== tp[i]) return false;
    }
    return pp.length === tp.length;
  }

}

// ---------- One-shot camera discovery ----------
//
// Strategy (in order of reliability):
//   1. Subscribe to `{prefix}/stats` — Frigate publishes this every few seconds
//      automatically. The JSON has a `cameras` key with camera names.
//   2. Subscribe to `{prefix}/camera_activity` — published in response to
//      `{prefix}/onConnect`. Handles both flat {cam:{}} and nested {cameras:{cam:{}}} formats.
//
// Returns an empty array on timeout so callers can fall back to manual entry.
// Only rejects on a hard connection failure (broker unreachable, auth error).

export function discoverFrigateCameras(
  brokerUrl: string,
  username: string | undefined,
  password: string | undefined,
  topicPrefix: string,
  timeoutMs: number = 12000,
): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const options: IClientOptions = {
      clientId: `homey-frigate-discover-${Date.now()}`,
      clean: true,
      connectTimeout: 8000,
    };
    if (username) options.username = username;
    if (password) options.password = password;

    const client = mqtt.connect(brokerUrl, options);
    let settled = false;
    let connectionFailed = false;

    const done = (result: string[] | Error): void => {
      if (settled) return;
      settled = true;
      client.end(true);
      if (result instanceof Error) reject(result);
      else resolve(result);
    };

    // On timeout: resolve with empty array so pairing continues with manual entry
    const timer = setTimeout(() => done([]), timeoutMs);

    client.on('connect', () => {
      client.subscribe(`${topicPrefix}/stats`);
      client.subscribe(`${topicPrefix}/camera_activity`);
      // Prompt Frigate to publish camera_activity after a short delay
      setTimeout(() => client.publish(`${topicPrefix}/onConnect`, ''), 400);
    });

    client.on('message', (topic: string, payload: Buffer) => {
      try {
        const data = JSON.parse(payload.toString());
        let cameras: string[] = [];

        if (topic === `${topicPrefix}/stats`) {
          // stats payload: { cameras: { cam_name: {...}, ... }, ... }
          if (data?.cameras && typeof data.cameras === 'object') {
            cameras = Object.keys(data.cameras);
          }
        } else if (topic === `${topicPrefix}/camera_activity`) {
          // Two possible formats Frigate may use:
          //   Flat:   { cam_name: { motion: false, objects: [] }, ... }
          //   Nested: { cameras: { cam_name: { ... }, ... } }
          if (data?.cameras && typeof data.cameras === 'object') {
            cameras = Object.keys(data.cameras);
          } else if (data && typeof data === 'object') {
            cameras = Object.keys(data).filter((k) => typeof data[k] === 'object');
          }
        }

        if (cameras.length > 0) {
          clearTimeout(timer);
          done(cameras);
        }
        // If cameras is still empty, keep waiting for the next message
      } catch { /* ignore malformed payloads */ }
    });

    client.on('error', (err: Error) => {
      // Only reject on connection errors (wrong host, auth failure, etc.)
      // Suppress subsequent errors after we've already settled
      if (settled || !connectionFailed) {
        connectionFailed = true;
        clearTimeout(timer);
        done(err instanceof Error ? err : new Error(String(err)));
      }
    });
  });
}
