import Homey from 'homey';

class FrigateApp extends Homey.App {

  async onInit() {
    this.log('Frigate NVR app has started');
  }

}

module.exports = FrigateApp;
