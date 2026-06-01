"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const homey_1 = __importDefault(require("homey"));
class FrigateApp extends homey_1.default.App {
    async onInit() {
        this.log('Frigate NVR app has started');
    }
}
module.exports = FrigateApp;
