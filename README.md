# Frigate NVR for Homey

Integrate your [Frigate NVR](https://frigate.video) instance with Homey for real-time motion detection, object alerts, and camera control via MQTT.

## Requirements

- A running **Frigate NVR** instance (v0.13+)
- An **MQTT broker** (e.g., Mosquitto, RabbitMQ) accessible to both Frigate and Homey
- At least one camera configured in Frigate

## Setup Instructions

### 1. Configure MQTT in Frigate

Frigate communicates with Homey through MQTT. Add the following to your `config.yml`:

```yaml
mqtt:
  enabled: True
  host: 192.168.0.254    # Replace with your MQTT broker IP
  user: frigate.         # MQTT username
  password: frigate_pass # MQTT password
  topic_prefix: frigate
  client_id: frigate
  stats_interval: 60
  qos: 0

detectors:
  onnx:
    type: onnx

model:
  path: /config/model_cache/yolov9-t-320.onnx
  model_type: yolo-generic
  width: 320
  height: 320
  input_tensor: nchw
  input_pixel_format: rgb
  input_dtype: float

cameras:
  c6n:
    ffmpeg:
      inputs:
        - path: rtsp://user:verif_code@192.168.x.x:554/ch1/main
          roles:
            - record
        - path: rtsp://user:verif_code@192.168.x.x:554/ch1/sub
          roles:
            - detect
  c1mini:
    ffmpeg:
      inputs:
        - path: rtsp://user:verif_code@192.168.x.x:554/ch1/main
          roles:
            - record
            - detect
    detect:
      enabled: true
      width: 1280
      height: 720
      fps: 5
    objects:
      track:
        - person
version: 0.17-0
```

### 2. Add Device in Homey

1. In Homey, add a new device (Camera)
2. Enter your mqtt broker (from example: mqtt://192.168.0.254:1883)
3. Enter user and password (from example: frigate / frigate_pass)
4. Enter your **Frigate server address** (e.g., `http://192.168.1.100:5000`)
If you want to be able to access the snapshot image from frigate object detection, you will need to configure a proxy such as traefik or caddy and enter the url here (e.g., `https://frigate-events.yourdomain.com`)
5. Select the camera you want to integrate and click add camera

## Features

### Motion & Object Detection

Receive triggers when Frigate detects motion or objects:

- **Object detected**: Fired when an object matching your filter is detected
- **New review alert**: Fired when Frigate flags an alert for manual review

Tokens include:
- Detected object label (e.g., "person")
- Confidence score (0-100%)
- Snapshot image
- Event & clip URLs (if Frigate URL is configured)

### Camera Control

Use flow actions to control recording and detection:

- Enable/disable object detection
- Enable/disable recording
- Enable/disable snapshots
- Suspend notifications for a specified duration
- Restart Frigate

### Device Status

Monitor camera activity:

- **Detection FPS**: Current frames-per-second for object detection
- **Events today**: Number of events detected today
- **Unreviewed alerts**: Count of alerts pending manual review

## Advanced: Remote Snapshot Access

To display snapshots in Homey flows from outside your local network, you need to expose your Frigate instance via a reverse proxy (e.g., nginx, Traefik, or cloud proxy).

### Configuration in Homey

Once your proxy is set up, set the **Frigate URL** to your external address:
- e.g., `https://frigate-events.yourdomain.com`

Snapshot and clip URLs in flow tokens will now include the full public URL, allowing Homey to fetch and display images.

For issues or feature requests, visit the [GitHub repository](https://github.com/drfatalis/com.drfatalis.frigate).
