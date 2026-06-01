import Homey from 'homey';
import { PairSession } from 'homey/lib/Driver';
import { discoverFrigateCameras } from '../../lib/MQTTClient';

interface CameraDevice extends Homey.Device {
  restart(): void;
  setDetection(enabled: boolean): void;
  setRecording(enabled: boolean): void;
  setSnapshots(enabled: boolean): void;
  suspendNotifications(minutes: number): void;
  wasLabelDetectedRecently(label: string, minutes: number): boolean;
  hasUnreviewedAlerts(): boolean;
}

interface PairSettings {
  cameras: string[];
  mqtt_url: string;
  mqtt_username: string;
  mqtt_password: string;
  mqtt_topic_prefix: string;
  frigate_url: string;
  frigate_local_url: string;
}

class CameraDriver extends Homey.Driver {

  private pairSettings: PairSettings = {
    cameras: [],
    mqtt_url: '',
    mqtt_username: '',
    mqtt_password: '',
    mqtt_topic_prefix: 'frigate',
    frigate_url: '',
    frigate_local_url: '',
  };

  async onInit() {
    this.log('CameraDriver initialized');

    this.homey.flow
      .getActionCard('restart-frigate')
      .registerRunListener(async (args: { device: CameraDevice }) => {
        args.device.restart();
      });

    this.homey.flow
      .getActionCard('set-detection')
      .registerRunListener(async (args: { device: CameraDevice; enabled: 'true' | 'false' }) => {
        args.device.setDetection(args.enabled === 'true');
      });

    this.homey.flow
      .getActionCard('set-recording')
      .registerRunListener(async (args: { device: CameraDevice; enabled: 'true' | 'false' }) => {
        args.device.setRecording(args.enabled === 'true');
      });

    this.homey.flow
      .getActionCard('set-snapshots')
      .registerRunListener(async (args: { device: CameraDevice; enabled: 'true' | 'false' }) => {
        args.device.setSnapshots(args.enabled === 'true');
      });

    this.homey.flow
      .getActionCard('suspend-notifications')
      .registerRunListener(async (args: { device: CameraDevice; minutes: number }) => {
        args.device.suspendNotifications(args.minutes);
      });

    this.homey.flow
      .getConditionCard('label-detected-recently')
      .registerRunListener((args: { device: CameraDevice; label: string; minutes: number }) => {
        return args.device.wasLabelDetectedRecently(args.label, args.minutes);
      });

    this.homey.flow
      .getConditionCard('camera-has-unreviewed-alerts')
      .registerRunListener((args: { device: CameraDevice }) => {
        return args.device.hasUnreviewedAlerts();
      });
  }

  async onPair(session: PairSession) {

    // Step 1: validate broker connection + attempt camera discovery
    session.setHandler('validate_credentials', async (data: {
      mqtt_url: string;
      mqtt_username: string;
      mqtt_password: string;
      mqtt_topic_prefix: string;
      frigate_url: string;
      frigate_local_url: string;
    }) => {
      const prefix = data.mqtt_topic_prefix?.trim() || 'frigate';

      // discoverFrigateCameras rejects only on hard broker errors;
      // returns [] if Frigate is not yet publishing / discovery timed out.
      const cameras = await discoverFrigateCameras(
        data.mqtt_url,
        data.mqtt_username || undefined,
        data.mqtt_password || undefined,
        prefix,
      );

      this.pairSettings = {
        cameras,
        mqtt_url: data.mqtt_url,
        mqtt_username: data.mqtt_username ?? '',
        mqtt_password: data.mqtt_password ?? '',
        mqtt_topic_prefix: prefix,
        frigate_url: data.frigate_url?.replace(/\/+$/, '') ?? '',
        frigate_local_url: data.frigate_local_url?.replace(/\/+$/, '') ?? '',
      };
    });

    // Step 2: camera view requests stored settings + discovered camera list
    session.setHandler('get_pair_settings', async () => {
      return this.pairSettings;
    });
  }

}

module.exports = CameraDriver;
