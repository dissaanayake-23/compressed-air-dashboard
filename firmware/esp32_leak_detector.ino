/*
 * =====================================================================================
 *  ESP32 + INMP441 Microphone — Compressed Air Acoustic Leak Detector
 *  Protocol: MQTT → HiveMQ Public Broker (broker.hivemq.com)
 *  Single Topic Architecture: acoustic-leak-detection/sensor-data
 * =====================================================================================
 *  Hardware Connections (ESP32 <-> INMP441 I2S Microphone):
 *    - VDD   -> 3.3V
 *    - GND   -> GND
 *    - L/R   -> GND (Left Channel)
 *    - WS    -> GPIO 15 (Word Select / LRCL)
 *    - SCK   -> GPIO 14 (Bit Clock / BCLK)
 *    - SD    -> GPIO 32 (Serial Data / DOUT)
 *
 *  Onboard Indicator LED: GPIO 2
 *
 *  Required Arduino Libraries:
 *    - PubSubClient  by Nick O'Leary    (MQTT Client)
 *    - ArduinoJson   by Benoit Blanchon (JSON serialization)
 * =====================================================================================
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <driver/i2s.h>
#include <math.h>

// ─────────────────────────────────────────────────────────────────────────────
//  CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────────

// WiFi Credentials
const char* WIFI_SSID     = "YOUR_WIFI_SSID";
const char* WIFI_PASSWORD = "YOUR_WIFI_PASSWORD";

// HiveMQ Public Broker (Free & No Account / Certificate Required)
const char* MQTT_BROKER   = "broker.hivemq.com";
const int   MQTT_PORT     = 1883;
const char* MQTT_CLIENT_ID = "esp32-acoustic-leak-node01"; // unique per ESP32

// Single MQTT Topic (matches Dashboard & Server)
#define TOPIC_TELEMETRY "acoustic-leak-detection/sensor-data"

// Node Identity & Acoustic Threshold
const char* NODE_ID          = "Node 01";  // Change per device: Node 01, Node 02, etc.
const float THRESHOLD_DB     = 70.0;       // Leak threshold in dB

// ─────────────────────────────────────────────────────────────────────────────
//  I2S PIN CONFIG (INMP441 Microphone)
// ─────────────────────────────────────────────────────────────────────────────
#define I2S_WS          15
#define I2S_SCK         14
#define I2S_SD          32
#define I2S_PORT        I2S_NUM_0
#define SAMPLE_RATE     44100
#define BUFFER_SAMPLES  512
#define LED_PIN         2

// ─────────────────────────────────────────────────────────────────────────────
//  TIMING
// ─────────────────────────────────────────────────────────────────────────────
#define PUBLISH_INTERVAL_MS 2500  // Send telemetry every 2.5 seconds

// ─────────────────────────────────────────────────────────────────────────────
//  GLOBALS
// ─────────────────────────────────────────────────────────────────────────────
WiFiClient   wifiClient;
PubSubClient mqttClient(wifiClient);

unsigned long lastPublishTime = 0;

// ─────────────────────────────────────────────────────────────────────────────
//  I2S INITIALIZATION
// ─────────────────────────────────────────────────────────────────────────────
void setupI2S() {
  i2s_config_t i2s_config = {
    .mode             = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_RX),
    .sample_rate      = SAMPLE_RATE,
    .bits_per_sample  = I2S_BITS_PER_SAMPLE_32BIT,
    .channel_format   = I2S_CHANNEL_FMT_ONLY_LEFT,
    .communication_format = i2s_comm_format_t(I2S_COMM_FORMAT_STAND_I2S),
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count    = 4,
    .dma_buf_len      = BUFFER_SAMPLES,
    .use_apll         = false,
    .tx_desc_auto_clear = false,
    .fixed_mclk       = 0
  };

  i2s_pin_config_t pin_config = {
    .bck_io_num      = I2S_SCK,
    .ws_io_num       = I2S_WS,
    .data_out_num    = I2S_PIN_NO_CHANGE,
    .data_in_num     = I2S_SD
  };

  i2s_driver_install(I2S_PORT, &i2s_config, 0, NULL);
  i2s_set_pin(I2S_PORT, &pin_config);
  Serial.println("[I2S] INMP441 microphone initialized successfully");
}

// ─────────────────────────────────────────────────────────────────────────────
//  SOUND LEVEL MEASUREMENT (RMS to Decibels)
// ─────────────────────────────────────────────────────────────────────────────
float measureSoundLevelDB() {
  int32_t samples[BUFFER_SAMPLES];
  size_t  bytes_read = 0;

  i2s_read(I2S_PORT, &samples, sizeof(samples), &bytes_read, portMAX_DELAY);
  int sample_count = bytes_read / sizeof(int32_t);

  if (sample_count == 0) return 40.0f;

  double sum_squares = 0.0;
  for (int i = 0; i < sample_count; i++) {
    double normalized = (double)samples[i] / 2147483648.0;
    sum_squares += normalized * normalized;
  }
  double rms = sqrt(sum_squares / sample_count);
  if (rms < 1e-10) return 30.0f;

  float db = 20.0f * log10f((float)rms) + 120.0f;
  return max(30.0f, min(120.0f, db));
}

// ─────────────────────────────────────────────────────────────────────────────
//  WIFI CONNECTION
// ─────────────────────────────────────────────────────────────────────────────
void connectWiFi() {
  Serial.printf("\n[WiFi] Connecting to %s", WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  int tries = 0;
  while (WiFi.status() != WL_CONNECTED && tries < 30) {
    delay(500);
    Serial.print(".");
    tries++;
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("\n[WiFi] Connected! IP: %s\n", WiFi.localIP().toString().c_str());
  } else {
    Serial.println("\n[WiFi] Connection failed — restarting in 5 s");
    delay(5000);
    ESP.restart();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
//  MQTT CONNECTION (HiveMQ Public Broker)
// ─────────────────────────────────────────────────────────────────────────────
void connectMQTT() {
  while (!mqttClient.connected()) {
    Serial.printf("[MQTT] Connecting to %s:1883 as %s ...", MQTT_BROKER, MQTT_CLIENT_ID);
    if (mqttClient.connect(MQTT_CLIENT_ID)) {
      Serial.println(" CONNECTED!");
      digitalWrite(LED_PIN, HIGH);
    } else {
      Serial.printf(" FAILED (rc=%d), retrying in 4 s\n", mqttClient.state());
      digitalWrite(LED_PIN, LOW);
      delay(4000);
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
//  PUBLISH — Single Topic Telemetry
// ─────────────────────────────────────────────────────────────────────────────
void publishTelemetry(float soundLevel, bool leakDetected) {
  // Only send nodeId, leakDetected, and status
  StaticJsonDocument<128> doc;
  doc["nodeId"]       = NODE_ID;
  doc["leakDetected"] = leakDetected;
  doc["status"]       = leakDetected ? "leak" : "normal";

  char payload[128];
  serializeJson(doc, payload);

  bool ok = mqttClient.publish(TOPIC_TELEMETRY, payload, false);
  Serial.printf("[MQTT] %s | Node=%s | status=%s | leakDetected=%s | %s\n",
    TOPIC_TELEMETRY, NODE_ID, leakDetected ? "leak" : "normal", leakDetected ? "true" : "false", ok ? "SENT" : "ERR");
}

// ─────────────────────────────────────────────────────────────────────────────
//  SETUP
// ─────────────────────────────────────────────────────────────────────────────
void setup() {
  Serial.begin(115200);
  delay(500);

  Serial.println("\n╔══════════════════════════════════════════════════╗");
  Serial.println("║  Acoustic Compressed Air Leak Detector           ║");
  Serial.println("║  ESP32 + INMP441 → HiveMQ Public Broker          ║");
  Serial.println("╚══════════════════════════════════════════════════╝");

  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);

  setupI2S();
  connectWiFi();

  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
  mqttClient.setKeepAlive(30);
  mqttClient.setBufferSize(512);
}

// ─────────────────────────────────────────────────────────────────────────────
//  MAIN LOOP
// ─────────────────────────────────────────────────────────────────────────────
void loop() {
  // Ensure WiFi is connected
  if (WiFi.status() != WL_CONNECTED) {
    connectWiFi();
  }

  // Ensure MQTT is connected
  if (!mqttClient.connected()) {
    connectMQTT();
  }
  mqttClient.loop();

  unsigned long now = millis();
  if (now - lastPublishTime >= PUBLISH_INTERVAL_MS) {
    lastPublishTime = now;

    float soundLevel   = measureSoundLevelDB();
    bool  leakDetected = (soundLevel >= THRESHOLD_DB);

    // Indicator LED: ON if leak detected, OFF if normal
    digitalWrite(LED_PIN, leakDetected ? HIGH : LOW);

    // Publish to the single topic
    publishTelemetry(soundLevel, leakDetected);
  }
}
