import Homey from 'homey';
import http from 'http';
import https from 'https';
import stream from 'stream';
import { FrigateMQTTClient } from '../../lib/MQTTClient';

interface DeviceSettings {
  mqtt_url: string;
  mqtt_username: string;
  mqtt_password: string;
  mqtt_topic_prefix: string;
  camera_name: string;
  frigate_url: string;
  frigate_local_url: string;
  label_filter: string;
  live_stream_interval: number;
  event_trigger_delay: number;
}

interface FrigateEventPayload {
  type: 'new' | 'update' | 'end';
  before: Record<string, unknown> | null;
  after: {
    id: string;
    camera: string;
    label: string;
    sub_label: string | null;
    score: number;
    top_score: number;
    start_time: number;
    end_time: number | null;
    current_zones: string[];
    entered_zones: string[];
    has_snapshot: boolean;
    has_clip: boolean;
  };
}

interface FrigateReviewPayload {
  type: 'new' | 'update' | 'end';
  before: Record<string, unknown> | null;
  after: {
    id: string;
    camera: string;
    severity: 'alert' | 'detection';
    has_been_reviewed: boolean;
    data: { objects: string[]; zones: string[]; audio: string[] };
  };
}

class CameraDevice extends Homey.Device {

  private settings: DeviceSettings = {
    mqtt_url: '',
    mqtt_username: '',
    mqtt_password: '',
    mqtt_topic_prefix: 'frigate',
    camera_name: '',
    frigate_url: '',
    frigate_local_url: '',
    label_filter: '',
    live_stream_interval: 500,
    event_trigger_delay: 5,
  };

  private mqtt: FrigateMQTTClient | null = null;
  private destroyed: boolean = false;

  private latestSnapshot: Buffer | null = null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private cameraImage: any = null;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private liveStreamImage: any = null;
  private liveStreamTimer: ReturnType<typeof setInterval> | null = null;

  // In-memory rolling window: label → array of unix timestamps (seconds)
  private recentDetections: Map<string, number[]> = new Map();

  // Per-label snapshot images for the device-card image picker
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private labelImages: Map<string, any> = new Map();
  private labelBuffers: Map<string, Buffer> = new Map();

  // Unreviewed alert counter (tracked from MQTT; reset when review_status=NONE)
  private unreviewedCount: number = 0;

  // Daily event counter
  private dailyEventCount: number = 0;
  private dailyCountDate: number = new Date().getDate();

  // Dedup guards
  private seenEventIds: Set<string> = new Set();
  private seenReviewIds: Set<string> = new Set();

  // Cooldown: unix ms timestamp of the last fired object-detected trigger
  private lastTriggerFiredAt: number = 0;

  // Prevents re-registering MQTT handlers on every reconnect
  private subscriptionsSetup: boolean = false;

  // ---------- Helpers ----------

  private loadSettings(): void {
    this.settings = this.getSettings() as DeviceSettings;
    if (!this.settings.mqtt_topic_prefix?.trim()) this.settings.mqtt_topic_prefix = 'frigate';
    this.settings.frigate_url = this.settings.frigate_url?.replace(/\/+$/, '') ?? '';
    this.settings.frigate_local_url = this.settings.frigate_local_url?.replace(/\/+$/, '') ?? '';
  }

  private get p(): string { return this.settings.mqtt_topic_prefix; }
  private get cam(): string { return this.settings.camera_name; }

  private get labelFilter(): string[] {
    if (!this.settings.label_filter?.trim()) return [];
    return this.settings.label_filter.split(',').map((l) => l.trim().toLowerCase()).filter(Boolean);
  }

  private snapshotUrl(eventId: string): string {
    const base = this.settings.frigate_url;
    return base ? `${base}/api/events/${eventId}/snapshot.jpg?bbox=1` : '';
  }

  private clipUrl(eventId: string): string {
    const base = this.settings.frigate_url;
    return base ? `${base}/api/events/${eventId}/clip.mp4` : '';
  }

  private fetchBuffer(url: string): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const get = url.startsWith('https://') ? https.get : http.get;
      get(url, (res) => {
        if (res.statusCode !== 200) {
          res.resume();
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }
        const chunks: Buffer[] = [];
        res.on('data', (chunk: Buffer) => chunks.push(chunk));
        res.on('end', () => resolve(Buffer.concat(chunks)));
        res.on('error', reject);
      }).on('error', reject);
    });
  }

  // ---------- Init / teardown ----------

  async onInit() {
    this.loadSettings();
    this.log(`[${this.cam}] Initializing — broker: ${this.settings.mqtt_url}, prefix: "${this.p}", filter: "${this.settings.label_filter || 'none'}", cooldown: ${this.settings.event_trigger_delay}s`);

    for (const cap of ['alarm_motion', 'detection_fps', 'event_count', 'unreviewed_alerts']) {
      if (!this.hasCapability(cap)) {
        await this.addCapability(cap);
        this.log(`[${this.cam}] Added missing capability: ${cap}`);
      }
    }
    await this.setCapabilityValue('alarm_motion', false);
    await this.setCapabilityValue('event_count', 0);
    await this.setCapabilityValue('unreviewed_alerts', 0);

    await this.initCameraImage();
    await this.initLiveStreamImage();
    this.startMQTT();
  }

  private async initCameraImage(): Promise<void> {
    try {
      this.cameraImage = await this.homey.images.createImage();
      this.cameraImage.setStream(async (imageStream: stream.Writable) => {
        const base = this.settings.frigate_local_url || this.settings.frigate_url;
        if (base) {
          const url = `${base}/api/${this.cam}/latest.jpg`;
          try {
            const buffer = await this.fetchBuffer(url);
            imageStream.end(buffer);
            this.log(`[${this.cam}] Camera image fetch succeeded (${url})`);
          } catch (err) {
            this.error(`[${this.cam}] Camera image fetch failed (${url}):`, err);
            imageStream.end();
          }
        } else if (this.latestSnapshot) {
          imageStream.end(this.latestSnapshot);
        } else {
          imageStream.end();
        }
      });
      await this.setCameraImage('snapshot', 'Latest Snapshot', this.cameraImage);
      this.log(`[${this.cam}] Camera snapshot image registered`);
    } catch (err) {
      this.error(`[${this.cam}] Failed to create camera image:`, err);
    }
  }

  private async initLiveStreamImage(): Promise<void> {
    const base = this.settings.frigate_local_url;
    if (!base) {
      this.log(`[${this.cam}] No local URL configured — live stream image skipped`);
      return;
    }
    try {
      if (!this.liveStreamImage) {
        this.liveStreamImage = await this.homey.images.createImage();
        await this.setCameraImage('live_stream', 'Live Stream', this.liveStreamImage);
      }
      if (base.startsWith('https://')) {
        this.stopLiveStreamTimer();
        this.liveStreamImage.setUrl(`${base}/api/${this.cam}/stream`);
        this.log(`[${this.cam}] Live stream: HTTPS MJPEG mode → ${base}/api/${this.cam}/stream`);
      } else {
        this.liveStreamImage.setStream(async (imageStream: stream.Writable) => {
          const url = `${base}/api/${this.cam}/latest.jpg`;
          try {
            const buffer = await this.fetchBuffer(url);
            imageStream.end(buffer);
          } catch (err) {
            this.error(`[${this.cam}] Live stream fetch failed (${url}):`, err);
            imageStream.end();
          }
        });
        const interval = Math.max(200, this.settings.live_stream_interval ?? 500);
        this.startLiveStreamTimer();
        this.log(`[${this.cam}] Live stream: HTTP poll mode, interval ${interval}ms`);
      }
      this.liveStreamImage.update().catch(() => {});
    } catch (err) {
      this.error(`[${this.cam}] Failed to create live stream image:`, err);
    }
  }

  private startLiveStreamTimer(): void {
    this.stopLiveStreamTimer();
    const interval = Math.max(200, this.settings.live_stream_interval ?? 500);
    this.liveStreamTimer = setInterval(() => {
      this.liveStreamImage?.update().catch(() => {});
    }, interval);
  }

  private stopLiveStreamTimer(): void {
    if (this.liveStreamTimer !== null) {
      clearInterval(this.liveStreamTimer);
      this.liveStreamTimer = null;
    }
  }

  private async updateLabelImage(label: string): Promise<void> {
    if (this.labelImages.has(label)) {
      this.labelImages.get(label).update().catch(() => {});
      return;
    }
    const img = await this.homey.images.createImage();
    img.setStream(async (imageStream: stream.Writable) => {
      imageStream.end(this.labelBuffers.get(label) ?? Buffer.alloc(0));
    });
    this.labelImages.set(label, img);
    const title = `${label.charAt(0).toUpperCase()}${label.slice(1)}`;
    await this.setCameraImage(`label_${label}`, `Latest ${title}`, img);
    this.log(`[${this.cam}] Registered label image: "${label}"`);
  }

  private startMQTT(): void {
    this.log(`[${this.cam}] Starting MQTT client → ${this.settings.mqtt_url}`);
    this.mqtt = new FrigateMQTTClient(
      this.settings.mqtt_url,
      this.settings.mqtt_username || undefined,
      this.settings.mqtt_password || undefined,
    );
    this.mqtt.connect(
      () => this.onMQTTConnected(),
      () => this.onMQTTDisconnected(),
    );
  }

  private stopMQTT(): void {
    this.log(`[${this.cam}] Stopping MQTT client`);
    this.mqtt?.disconnect();
    this.mqtt = null;
    this.subscriptionsSetup = false;
  }

  // ---------- MQTT connection ----------

  private async onMQTTConnected(): Promise<void> {
    if (!this.subscriptionsSetup) {
      this.subscriptionsSetup = true;
      this.log(`[${this.cam}] MQTT connected — registering subscriptions`);

      this.mqtt!.subscribe(`${this.p}/available`, (_t, payload) => {
        this.log(`[${this.cam}] Frigate server availability: ${payload}`);
        if (payload === 'online') this.setAvailable().catch(() => {});
        else this.setUnavailable(this.homey.__('device.unavailable')).catch(() => {});
      });

      this.mqtt!.subscribe(`${this.p}/${this.cam}/motion`, (_t, payload) => {
        this.log(`[${this.cam}] Motion state: ${payload}`);
        this.setCapabilityValue('alarm_motion', payload === 'ON').catch(() => {});
      });

      this.mqtt!.subscribe(`${this.p}/${this.cam}/review_status`, (_t, payload) => {
        this.log(`[${this.cam}] Review status: ${payload}`);
        if (payload === 'NONE') {
          this.unreviewedCount = 0;
          this.setCapabilityValue('unreviewed_alerts', 0).catch(() => {});
        }
      });

      this.mqtt!.subscribe(`${this.p}/stats`, (_t, payload) => {
        try {
          const stats = JSON.parse(payload);
          const fps = stats?.cameras?.[this.cam]?.detection_fps;
          if (typeof fps === 'number') {
            this.setCapabilityValue('detection_fps', fps).catch(() => {});
          }
        } catch { /* ignore */ }
      });

      this.mqtt!.subscribe(`${this.p}/events`, (_t, payload) => {
        this.handleEvent(payload).catch((err: unknown) => this.error(`[${this.cam}] handleEvent error:`, err));
      });

      this.mqtt!.subscribe(`${this.p}/reviews`, (_t, payload) => this.handleReview(payload));

      this.mqtt!.subscribeBinary(`${this.p}/${this.cam}/+/snapshot`, (topic, payload) => {
        this.latestSnapshot = payload;
        this.cameraImage?.update().catch(() => {});
        const parts = topic.split('/');
        const label = parts[parts.length - 2];
        if (label) {
          this.labelBuffers.set(label, payload);
          this.updateLabelImage(label).catch((err: unknown) => this.error(`[${this.cam}] Label image update failed:`, err));
        }
      });

      this.log(`[${this.cam}] Subscriptions active`);
    } else {
      this.log(`[${this.cam}] MQTT reconnected`);
    }
    await this.setAvailable();
  }

  private async onMQTTDisconnected(): Promise<void> {
    this.log(`[${this.cam}] MQTT disconnected — device marked unavailable`);
    await this.setUnavailable(this.homey.__('device.unavailable'));
    await this.setCapabilityValue('alarm_motion', false);
  }

  // ---------- MQTT message handlers ----------

  private async handleEvent(payload: string): Promise<void> {
    let msg: FrigateEventPayload;
    try { msg = JSON.parse(payload); } catch { return; }

    if (msg.type !== 'end') return;
    const ev = msg.after;
    if (ev.camera !== this.cam) return;

    const score = Math.round((ev.top_score ?? ev.score ?? 0) * 100);
    this.log(`[${this.cam}] Event ended: id=${ev.id} label="${ev.label}" score=${score}% sub_label="${ev.sub_label ?? '-'}" zones=[${[...(ev.current_zones ?? []), ...(ev.entered_zones ?? [])].join(', ') || 'none'}] snapshot=${ev.has_snapshot} clip=${ev.has_clip}`);

    // Discard stale retained/replayed events (ended more than 60 s ago)
    if (ev.end_time !== null) {
      const ageSec = Date.now() / 1000 - ev.end_time;
      if (ageSec > 60) {
        this.log(`[${this.cam}] Event ${ev.id} discarded — stale (ended ${ageSec.toFixed(0)}s ago, likely a retained MQTT message)`);
        return;
      }
    }

    // Rotate daily counter at midnight
    const today = new Date().getDate();
    if (today !== this.dailyCountDate) {
      this.log(`[${this.cam}] Midnight rollover — resetting daily counters and dedup set`);
      this.dailyEventCount = 0;
      this.dailyCountDate = today;
      this.seenEventIds.clear();
      this.recentDetections.clear();
    }

    if (this.seenEventIds.has(ev.id)) {
      this.log(`[${this.cam}] Event ${ev.id} discarded — duplicate`);
      return;
    }
    this.seenEventIds.add(ev.id);
    this.dailyEventCount++;
    this.setCapabilityValue('event_count', this.dailyEventCount).catch(() => {});

    // Track label using detection start time for label-detected-recently condition
    const labelKey = (ev.label ?? '').toLowerCase();
    if (!this.recentDetections.has(labelKey)) this.recentDetections.set(labelKey, []);
    this.recentDetections.get(labelKey)!.push(ev.start_time ?? Date.now() / 1000);

    // Apply optional label filter
    if (this.labelFilter.length > 0 && !this.labelFilter.includes(labelKey)) {
      this.log(`[${this.cam}] Event ${ev.id} discarded — label "${labelKey}" not in filter [${this.labelFilter.join(', ')}]`);
      return;
    }

    const zones = [...new Set([...(ev.current_zones ?? []), ...(ev.entered_zones ?? [])])].join(', ');
    const cooldownMs = Math.max(0, this.settings.event_trigger_delay ?? 5) * 1000;

    if (cooldownMs > 0 && Date.now() - this.lastTriggerFiredAt < cooldownMs) {
      const remainingSec = ((cooldownMs - (Date.now() - this.lastTriggerFiredAt)) / 1000).toFixed(1);
      this.log(`[${this.cam}] Event ${ev.id} discarded — cooldown active (${remainingSec}s remaining)`);
      return;
    }
    this.lastTriggerFiredAt = Date.now();

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let eventSnapshot: any = this.cameraImage;
    const snapUrl = this.snapshotUrl(ev.id);
    if (snapUrl && ev.has_snapshot) {
      try {
        const img = await this.homey.images.createImage();
        img.setStream(async (imageStream: stream.Writable) => {
          try {
            const buffer = await this.fetchBuffer(snapUrl);
            imageStream.end(buffer);
            this.log(`[${this.cam}] Event snapshot fetch succeeded (${snapUrl})`);
          } catch (err) {
            this.error(`[${this.cam}] Event snapshot fetch failed (${snapUrl}):`, err);
            imageStream.end(this.latestSnapshot ?? Buffer.alloc(0));
          }
        });
        eventSnapshot = img;
      } catch (err) {
        this.error(`[${this.cam}] Failed to create event image:`, err);
      }
    }

    const triggerTokens = {
      label: ev.label ?? '',
      sub_label: ev.sub_label ?? '',
      zones,
      score,
      event_id: ev.id,
      snapshot_url: snapUrl,
      clip_url: this.clipUrl(ev.id),
      snapshot: eventSnapshot,
      device_name: this.getName(),
    };

    this.log(`[${this.cam}] TRIGGER object-detected — id=${ev.id} label="${ev.label}" sub_label="${ev.sub_label ?? '-'}" score=${score}% zones=[${zones || 'none'}]`);

    this.homey.flow.getDeviceTriggerCard('object-detected')
      .trigger(this, triggerTokens)
      .catch((err: unknown) => this.error(`[${this.cam}] object-detected trigger error:`, err));

    this.homey.flow.getTriggerCard('object-detected-any')
      .trigger(triggerTokens)
      .catch((err: unknown) => this.error(`[${this.cam}] object-detected-any trigger error:`, err));

    if (this.seenEventIds.size > 500) {
      this.seenEventIds = new Set([...this.seenEventIds].slice(-250));
    }
  }

  private handleReview(payload: string): void {
    let msg: FrigateReviewPayload;
    try { msg = JSON.parse(payload); } catch { return; }

    if (msg.type !== 'new') return;
    const rev = msg.after;
    if (rev.camera !== this.cam) return;

    if (this.seenReviewIds.has(rev.id)) {
      this.log(`[${this.cam}] Review ${rev.id} discarded — duplicate`);
      return;
    }
    this.seenReviewIds.add(rev.id);

    if (!rev.has_been_reviewed) {
      this.unreviewedCount++;
      this.setCapabilityValue('unreviewed_alerts', this.unreviewedCount).catch(() => {});
      this.log(`[${this.cam}] Unreviewed alert count: ${this.unreviewedCount}`);
    }

    if (rev.severity === 'alert') {
      const objects = (rev.data?.objects ?? []).join(', ');
      const zones = (rev.data?.zones ?? []).join(', ');
      this.log(`[${this.cam}] TRIGGER review-alert — id=${rev.id} severity=${rev.severity} objects=[${objects || 'none'}] zones=[${zones || 'none'}]`);
      this.homey.flow.getDeviceTriggerCard('review-alert').trigger(this, {
        review_id: rev.id,
        severity: rev.severity,
        objects,
        zones,
      }).catch((err: unknown) => this.error(`[${this.cam}] review-alert trigger error:`, err));
    }

    if (this.seenReviewIds.size > 500) {
      this.seenReviewIds = new Set([...this.seenReviewIds].slice(-250));
    }
  }

  // ---------- Flow condition handlers ----------

  wasLabelDetectedRecently(label: string, minutes: number): boolean {
    const cutoff = Date.now() / 1000 - minutes * 60;
    const key = label.trim().toLowerCase();
    const timestamps = this.recentDetections.get(key) ?? [];
    const fresh = timestamps.filter((t) => t >= cutoff);
    this.recentDetections.set(key, fresh);
    this.log(`[${this.cam}] Condition label-detected-recently: label="${key}" window=${minutes}min → ${fresh.length} detection(s) found`);
    return fresh.length > 0;
  }

  hasUnreviewedAlerts(): boolean {
    return this.unreviewedCount > 0;
  }

  // ---------- Flow action handlers (all via MQTT publish) ----------

  private pub(topic: string, payload: string = ''): void {
    if (!this.mqtt) throw new Error('MQTT not connected');
    this.log(`[${this.cam}] MQTT publish → ${topic}${payload ? ` = ${payload}` : ''}`);
    this.mqtt.publish(topic, payload);
  }

  restart(): void          { this.pub(`${this.p}/restart`); }
  setDetection(on: boolean): void  { this.pub(`${this.p}/${this.cam}/detect/set`, on ? 'ON' : 'OFF'); }
  setRecording(on: boolean): void  { this.pub(`${this.p}/${this.cam}/recordings/set`, on ? 'ON' : 'OFF'); }
  setSnapshots(on: boolean): void  { this.pub(`${this.p}/${this.cam}/snapshots/set`, on ? 'ON' : 'OFF'); }
  suspendNotifications(minutes: number): void {
    this.pub(`${this.p}/${this.cam}/notifications/suspend`, String(Math.round(minutes)));
  }

  // ---------- Lifecycle ----------

  async onAdded() {
    this.log(`[${this.cam}] Device added to Homey`);
  }

  async onSettings({ changedKeys }: {
    oldSettings: Record<string, string | number | boolean | null | undefined>;
    newSettings: Record<string, string | number | boolean | null | undefined>;
    changedKeys: string[];
  }): Promise<string | void> {
    this.log(`[${this.cam}] Settings changed (${changedKeys.join(', ')}) — reconnecting`);
    this.stopMQTT();
    this.stopLiveStreamTimer();
    this.loadSettings();
    this.seenEventIds.clear();
    this.seenReviewIds.clear();
    this.recentDetections.clear();
    this.labelImages.clear();
    this.labelBuffers.clear();
    this.unreviewedCount = 0;
    this.lastTriggerFiredAt = 0;
    this.startMQTT();
    this.cameraImage?.update().catch(() => {});
    await this.initLiveStreamImage();
  }

  async onRenamed(name: string) {
    this.log(`[${this.cam}] Renamed to "${name}"`);
  }

  async onDeleted() {
    this.log(`[${this.cam}] Device deleted`);
    this.destroyed = true;
    this.stopMQTT();
    this.stopLiveStreamTimer();
  }

}

module.exports = CameraDevice;
