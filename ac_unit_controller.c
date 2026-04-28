/**
 * @file    ac_unit_controller.c
 * @brief   Production HVAC AC Unit Firmware
 *
 * Target:  ESP32 / STM32 running FreeRTOS
 * Language: C (C99)
 *
 * Architecture
 * ─────────────
 *
 *  Sensor ISR / Timer
 *       │
 *  xQueueSend(sensor_queue)
 *       │
 *  ┌────▼─────────────────────────────────┐
 *  │  PID Task  (1 Hz – high priority)    │
 *  │  • Reads sensor queue                │
 *  │  • Runs incremental PID              │
 *  │  • Applies output to actuators       │
 *  │  • Publishes telemetry               │
 *  └──────────────────────────────────────┘
 *
 *  ┌──────────────────────────────────────┐
 *  │  MQTT Task  (medium priority)        │
 *  │  • Maintains broker connection       │
 *  │  • Subscribes to command topic       │
 *  │  • Updates shared setpoint via mutex │
 *  │  • Sends heartbeat every 5s         │
 *  └──────────────────────────────────────┘
 *
 *  ┌──────────────────────────────────────┐
 *  │  Watchdog Task  (low priority)       │
 *  │  • HW watchdog feed                  │
 *  │  • Detects MQTT broker loss          │
 *  │  • Switches to LOCAL_AUTONOMY mode   │
 *  └──────────────────────────────────────┘
 *
 * Fault tolerance
 * ───────────────
 * If the MQTT broker is unreachable for > BROKER_TIMEOUT_MS, the unit
 * enters LOCAL_AUTONOMY mode: the PID controller continues running using
 * the last known setpoint. Telemetry is buffered in a ring buffer and
 * replayed when connectivity is restored.
 */

#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* FreeRTOS */
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"
#include "freertos/timers.h"
#include "freertos/event_groups.h"

/* Platform HAL (replace with target BSP) */
#include "hal/gpio.h"
#include "hal/adc.h"
#include "hal/pwm.h"

/* MQTT client (Paho Embedded-C or ESP-MQTT) */
#include "mqtt_client.h"

/* JSON (cJSON or ArduinoJSON) */
#include "cJSON.h"

/* ═══════════════════════════════════════════════════════════════════════════
 *  Configuration constants
 * ═══════════════════════════════════════════════════════════════════════════ */

#define UNIT_ID                 "ac-unit-001"
#define FW_VERSION              "1.0.0"
#define ZONE_ID                 "zone-floor-1-north"

#define MQTT_BROKER_HOST        "emqx.hvac.internal"
#define MQTT_BROKER_PORT        8883
#define MQTT_USERNAME           "unit-001"
#define MQTT_PASSWORD           "changeme"
#define MQTT_KEEPALIVE_S        60
#define MQTT_QOS                1

#define TOPIC_TELEMETRY         "ac/unit/" UNIT_ID "/telemetry"
#define TOPIC_COMMAND           "ac/unit/" UNIT_ID "/command"
#define TOPIC_HEARTBEAT         "ac/unit/" UNIT_ID "/heartbeat"
#define TOPIC_RESPONSE          "ac/unit/" UNIT_ID "/response"

/* PID tuning – tune per unit type / refrigerant */
#define PID_KP                  2.0f
#define PID_KI                  0.5f
#define PID_KD                  0.1f
#define PID_OUTPUT_MIN         -100.0f   /* cooling demand % */
#define PID_OUTPUT_MAX          100.0f
#define PID_INTEGRAL_CLAMP      50.0f   /* anti-windup */
#define PID_DT_S                1.0f    /* sample period  */
#define PID_DEAD_BAND_C         0.2f    /* ignore tiny errors */

/* Safety limits */
#define TEMP_MAX_SAFE_C         40.0f
#define TEMP_MIN_SAFE_C         10.0f
#define COMPRESSOR_MIN_OFF_MS   180000  /* 3-minute compressor guard */

/* Autonomy */
#define BROKER_TIMEOUT_MS       30000   /* declare offline after 30s */
#define HEARTBEAT_INTERVAL_MS   5000
#define TELEMETRY_INTERVAL_MS   2000
#define OFFLINE_BUFFER_SLOTS    256

/* Task parameters */
#define STACK_PID               4096
#define STACK_MQTT              8192
#define STACK_WATCHDOG          2048
#define PRIORITY_PID            5
#define PRIORITY_MQTT           4
#define PRIORITY_WATCHDOG       2

/* Event group bits */
#define EVT_MQTT_CONNECTED      BIT0
#define EVT_SETPOINT_CHANGED    BIT1
#define EVT_EMERGENCY_STOP      BIT2


/* ═══════════════════════════════════════════════════════════════════════════
 *  Data structures
 * ═══════════════════════════════════════════════════════════════════════════ */

typedef enum {
    MODE_ONLINE,          /* Connected to broker, commands accepted */
    MODE_LOCAL_AUTONOMY,  /* Broker unreachable, PID runs on last setpoint */
    MODE_EMERGENCY_STOP,  /* Overtemperature or sensor fault */
} OperatingMode_t;

typedef struct {
    float temperature;    /* °C  */
    float humidity;       /* %RH */
    float pressure;       /* hPa */
    float load_pct;       /* %   */
    float power_w;        /* W   */
    uint32_t timestamp_ms;
    uint8_t error_flags;  /* bit 0=temp_fault, 1=humid_fault, 2=pressure_fault */
} SensorReading_t;

typedef struct {
    float setpoint;
    float fan_speed_pct;  /* 0-100 */
    bool  on;
    char  issued_by[32];
    char  correlation_id[37];
} ControlCommand_t;

typedef struct {
    /* PID state */
    float kp, ki, kd;
    float integral;
    float prev_error;
    float dt;
    float output_min, output_max;
    float integral_clamp;
    bool  first_run;
} PIDController_t;

typedef struct {
    char   topic[128];
    char   payload[512];
    uint8_t qos;
} OfflineMessage_t;

/* Ring buffer for offline telemetry */
typedef struct {
    OfflineMessage_t slots[OFFLINE_BUFFER_SLOTS];
    volatile uint16_t head;
    volatile uint16_t tail;
    volatile uint16_t count;
} RingBuffer_t;


/* ═══════════════════════════════════════════════════════════════════════════
 *  Globals (protected by mutex or accessed from a single task)
 * ═══════════════════════════════════════════════════════════════════════════ */

static volatile OperatingMode_t g_mode = MODE_LOCAL_AUTONOMY;
static volatile uint32_t g_last_broker_ping_ms = 0;
static volatile uint32_t g_compressor_last_on_ms = 0;
static volatile bool     g_compressor_on = false;

static SemaphoreHandle_t  g_setpoint_mutex;
static ControlCommand_t   g_command;         /* protected by mutex */

static QueueHandle_t      g_sensor_queue;    /* SensorReading_t */
static EventGroupHandle_t g_events;

static RingBuffer_t       g_offline_buf;
static SemaphoreHandle_t  g_offline_buf_mutex;

static mqtt_client_handle_t g_mqtt;


/* ═══════════════════════════════════════════════════════════════════════════
 *  PID controller
 * ═══════════════════════════════════════════════════════════════════════════ */

static void pid_init(PIDController_t *pid,
                     float kp, float ki, float kd,
                     float out_min, float out_max,
                     float integral_clamp, float dt)
{
    pid->kp             = kp;
    pid->ki             = ki;
    pid->kd             = kd;
    pid->output_min     = out_min;
    pid->output_max     = out_max;
    pid->integral_clamp = integral_clamp;
    pid->dt             = dt;
    pid->integral       = 0.0f;
    pid->prev_error     = 0.0f;
    pid->first_run      = true;
}

/**
 * @brief  Incremental PID with anti-windup and derivative kick prevention.
 *
 * Uses the "derivative on measurement" variant to avoid sudden output
 * spikes when the setpoint changes.
 *
 * @param pid        PID state (modified in-place)
 * @param setpoint   Desired temperature (°C)
 * @param measured   Measured temperature (°C)
 * @return           Control output in [output_min, output_max]
 *                   Positive = more cooling demand
 */
static float pid_compute(PIDController_t *pid, float setpoint, float measured)
{
    float error      = setpoint - measured;

    /* Dead-band: suppress micro-corrections */
    if (fabsf(error) < PID_DEAD_BAND_C) {
        return pid->integral * pid->ki;   /* keep integral contribution */
    }

    /* Proportional */
    float p_term = pid->kp * error;

    /* Integral with clamping (anti-windup) */
    pid->integral += error * pid->dt;
    if (pid->integral >  pid->integral_clamp) pid->integral =  pid->integral_clamp;
    if (pid->integral < -pid->integral_clamp) pid->integral = -pid->integral_clamp;
    float i_term = pid->ki * pid->integral;

    /* Derivative on measurement (not on error) to prevent setpoint-change kicks */
    float derivative = pid->first_run ? 0.0f : (measured - pid->prev_error) / pid->dt;
    pid->first_run   = false;
    float d_term     = -pid->kd * derivative;   /* negative: rising temp → more cooling */

    pid->prev_error = measured;

    float output = p_term + i_term + d_term;

    /* Clamp output */
    if (output > pid->output_max) output = pid->output_max;
    if (output < pid->output_min) output = pid->output_min;

    return output;
}


/* ═══════════════════════════════════════════════════════════════════════════
 *  Actuator control
 * ═══════════════════════════════════════════════════════════════════════════ */

static void apply_control_output(float pid_output, float fan_speed_pct)
{
    /* Guard minimum compressor off-time (protects compressor from short-cycling) */
    uint32_t now = xTaskGetTickCount() * portTICK_PERIOD_MS;

    if (pid_output > 0.0f) {  /* cooling demand */
        if (!g_compressor_on) {
            uint32_t off_time = now - g_compressor_last_on_ms;
            if (off_time < COMPRESSOR_MIN_OFF_MS) {
                /* Not yet allowed to restart – reduce fan instead */
                hal_pwm_set_duty(PWM_FAN, (uint8_t)(fan_speed_pct * 0.5f));
                return;
            }
            hal_gpio_write(GPIO_COMPRESSOR, 1);
            g_compressor_on = true;
        }
        /* Map PID output (0-100) to compressor speed via PWM */
        uint8_t compressor_duty = (uint8_t)fminf(pid_output, 100.0f);
        hal_pwm_set_duty(PWM_COMPRESSOR, compressor_duty);
    } else {
        if (g_compressor_on) {
            hal_gpio_write(GPIO_COMPRESSOR, 0);
            g_compressor_on = false;
            g_compressor_last_on_ms = now;
        }
        hal_pwm_set_duty(PWM_COMPRESSOR, 0);
    }

    hal_pwm_set_duty(PWM_FAN, (uint8_t)fan_speed_pct);
}


/* ═══════════════════════════════════════════════════════════════════════════
 *  Offline ring buffer
 * ═══════════════════════════════════════════════════════════════════════════ */

static void rb_push(RingBuffer_t *rb, const char *topic, const char *payload, uint8_t qos)
{
    if (xSemaphoreTake(g_offline_buf_mutex, pdMS_TO_TICKS(10)) != pdTRUE) return;
    if (rb->count < OFFLINE_BUFFER_SLOTS) {
        OfflineMessage_t *slot = &rb->slots[rb->head];
        strncpy(slot->topic,   topic,   sizeof(slot->topic)   - 1);
        strncpy(slot->payload, payload, sizeof(slot->payload) - 1);
        slot->qos = qos;
        rb->head  = (rb->head + 1) % OFFLINE_BUFFER_SLOTS;
        rb->count++;
    }
    xSemaphoreGive(g_offline_buf_mutex);
}

static bool rb_pop(RingBuffer_t *rb, OfflineMessage_t *out)
{
    bool ret = false;
    if (xSemaphoreTake(g_offline_buf_mutex, pdMS_TO_TICKS(10)) == pdTRUE) {
        if (rb->count > 0) {
            *out     = rb->slots[rb->tail];
            rb->tail = (rb->tail + 1) % OFFLINE_BUFFER_SLOTS;
            rb->count--;
            ret = true;
        }
        xSemaphoreGive(g_offline_buf_mutex);
    }
    return ret;
}


/* ═══════════════════════════════════════════════════════════════════════════
 *  MQTT helpers
 * ═══════════════════════════════════════════════════════════════════════════ */

static void safe_publish(const char *topic, const char *payload, uint8_t qos, bool retain)
{
    if (g_mode == MODE_ONLINE) {
        mqtt_client_publish(g_mqtt, topic, payload, strlen(payload), qos, retain);
    } else {
        rb_push(&g_offline_buf, topic, payload, qos);
    }
}

static void build_telemetry_json(char *buf, size_t len, const SensorReading_t *s,
                                  float pid_out, OperatingMode_t mode)
{
    snprintf(buf, len,
        "{"
            "\"unit_id\":\"%s\","
            "\"timestamp\":%lu,"
            "\"temperature\":%.2f,"
            "\"humidity\":%.1f,"
            "\"pressure\":%.1f,"
            "\"load\":%.1f,"
            "\"power_w\":%.1f,"
            "\"pid_output\":%.2f,"
            "\"local_mode\":%s,"
            "\"error_flags\":%u"
        "}",
        UNIT_ID,
        (unsigned long)s->timestamp_ms,
        s->temperature, s->humidity, s->pressure,
        s->load_pct, s->power_w,
        pid_out,
        mode != MODE_ONLINE ? "true" : "false",
        s->error_flags
    );
}

static void build_heartbeat_json(char *buf, size_t len, OperatingMode_t mode)
{
    uint32_t uptime = xTaskGetTickCount() * portTICK_PERIOD_MS / 1000;
    snprintf(buf, len,
        "{"
            "\"unit_id\":\"%s\","
            "\"timestamp\":%lu,"
            "\"uptime_s\":%lu,"
            "\"fw_version\":\"%s\","
            "\"local_mode\":%s,"
            "\"buffer_count\":%u"
        "}",
        UNIT_ID,
        (unsigned long)(xTaskGetTickCount() * portTICK_PERIOD_MS),
        (unsigned long)uptime,
        FW_VERSION,
        mode != MODE_ONLINE ? "true" : "false",
        g_offline_buf.count
    );
}

/* MQTT event handler (called from MQTT task context) */
static void mqtt_event_handler(void *arg, esp_event_base_t base,
                                int32_t event_id, void *event_data)
{
    esp_mqtt_event_handle_t event = event_data;
    char payload_buf[512];

    switch (event_id) {
    case MQTT_EVENT_CONNECTED:
        g_last_broker_ping_ms = xTaskGetTickCount() * portTICK_PERIOD_MS;
        g_mode = MODE_ONLINE;
        xEventGroupSetBits(g_events, EVT_MQTT_CONNECTED);
        mqtt_client_subscribe(g_mqtt, TOPIC_COMMAND, MQTT_QOS);
        break;

    case MQTT_EVENT_DISCONNECTED:
        xEventGroupClearBits(g_events, EVT_MQTT_CONNECTED);
        /* Watchdog task will detect timeout and switch mode */
        break;

    case MQTT_EVENT_DATA:
        /* Command received */
        if (strncmp(event->topic, TOPIC_COMMAND, event->topic_len) == 0) {
            memset(payload_buf, 0, sizeof(payload_buf));
            size_t copy_len = event->data_len < sizeof(payload_buf) - 1
                              ? event->data_len : sizeof(payload_buf) - 1;
            memcpy(payload_buf, event->data, copy_len);

            cJSON *root = cJSON_ParseWithLength(payload_buf, copy_len);
            if (root) {
                if (xSemaphoreTake(g_setpoint_mutex, pdMS_TO_TICKS(100)) == pdTRUE) {
                    cJSON *sp   = cJSON_GetObjectItem(root, "target_temp");
                    cJSON *fan  = cJSON_GetObjectItem(root, "fan_speed_pct");
                    cJSON *on   = cJSON_GetObjectItem(root, "status");
                    cJSON *corr = cJSON_GetObjectItem(root, "correlation_id");

                    if (sp  && cJSON_IsNumber(sp))  g_command.setpoint      = (float)sp->valuedouble;
                    if (fan && cJSON_IsNumber(fan))  g_command.fan_speed_pct = (float)fan->valuedouble;
                    if (on  && cJSON_IsString(on))   g_command.on            = strcmp(on->valuestring, "ON") == 0;
                    if (corr && cJSON_IsString(corr))
                        strncpy(g_command.correlation_id, corr->valuestring, 36);

                    xSemaphoreGive(g_setpoint_mutex);
                    xEventGroupSetBits(g_events, EVT_SETPOINT_CHANGED);
                }
                cJSON_Delete(root);

                /* ACK */
                snprintf(payload_buf, sizeof(payload_buf),
                    "{\"unit_id\":\"%s\",\"status\":\"APPLIED\",\"correlation_id\":\"%s\"}",
                    UNIT_ID, g_command.correlation_id);
                mqtt_client_publish(g_mqtt, TOPIC_RESPONSE, payload_buf, strlen(payload_buf), 1, false);
            }
        }
        break;

    default:
        break;
    }
}


/* ═══════════════════════════════════════════════════════════════════════════
 *  FreeRTOS Tasks
 * ═══════════════════════════════════════════════════════════════════════════ */

/**
 * PID Task – highest priority, runs at 1 Hz
 */
static void task_pid(void *arg)
{
    PIDController_t pid;
    pid_init(&pid, PID_KP, PID_KI, PID_KD,
             PID_OUTPUT_MIN, PID_OUTPUT_MAX,
             PID_INTEGRAL_CLAMP, PID_DT_S);

    SensorReading_t reading;
    char json_buf[512];
    TickType_t last_telemetry = xTaskGetTickCount();

    while (1) {
        /* Block until sensor reading available (or 1.1s timeout) */
        if (xQueueReceive(g_sensor_queue, &reading, pdMS_TO_TICKS(1100)) != pdTRUE) {
            /* Sensor fault */
            reading.error_flags |= 0x01;
            xEventGroupSetBits(g_events, EVT_EMERGENCY_STOP);
        }

        /* Safety gate */
        if (g_mode == MODE_EMERGENCY_STOP || !g_command.on) {
            apply_control_output(0.0f, 0.0f);
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }

        /* Overtemperature protection */
        if (reading.temperature > TEMP_MAX_SAFE_C) {
            xEventGroupSetBits(g_events, EVT_EMERGENCY_STOP);
            apply_control_output(0.0f, 100.0f);  /* max fan, no compressor */
            continue;
        }

        /* Get current command (setpoint) */
        float setpoint, fan_speed;
        if (xSemaphoreTake(g_setpoint_mutex, pdMS_TO_TICKS(10)) == pdTRUE) {
            setpoint  = g_command.setpoint;
            fan_speed = g_command.fan_speed_pct;
            xSemaphoreGive(g_setpoint_mutex);
        } else {
            setpoint  = 22.0f;
            fan_speed = 50.0f;
        }

        /* Run PID */
        float output = pid_compute(&pid, setpoint, reading.temperature);

        /* Actuate */
        apply_control_output(output, fan_speed);

        /* Publish telemetry at TELEMETRY_INTERVAL_MS */
        TickType_t now = xTaskGetTickCount();
        if ((now - last_telemetry) * portTICK_PERIOD_MS >= TELEMETRY_INTERVAL_MS) {
            last_telemetry = now;
            build_telemetry_json(json_buf, sizeof(json_buf), &reading, output, g_mode);
            safe_publish(TOPIC_TELEMETRY, json_buf, MQTT_QOS, false);
        }

        vTaskDelay(pdMS_TO_TICKS(1000));  /* 1 Hz */
    }
}

/**
 * MQTT Task – manages broker connection and replays offline buffer
 */
static void task_mqtt(void *arg)
{
    /* Configure and start MQTT client */
    esp_mqtt_client_config_t mqtt_cfg = {
        .broker.address.uri       = "mqtts://" MQTT_BROKER_HOST,
        .broker.address.port      = MQTT_BROKER_PORT,
        .credentials.username     = MQTT_USERNAME,
        .credentials.authentication.password = MQTT_PASSWORD,
        .session.keepalive        = MQTT_KEEPALIVE_S,
        .network.reconnect_timeout_ms = 5000,
    };
    g_mqtt = esp_mqtt_client_init(&mqtt_cfg);
    esp_mqtt_client_register_event(g_mqtt, ESP_EVENT_ANY_ID, mqtt_event_handler, NULL);
    esp_mqtt_client_start(g_mqtt);

    char hb_buf[256];
    TickType_t last_hb = xTaskGetTickCount();

    while (1) {
        /* Heartbeat */
        TickType_t now = xTaskGetTickCount();
        if ((now - last_hb) * portTICK_PERIOD_MS >= HEARTBEAT_INTERVAL_MS) {
            last_hb = now;
            build_heartbeat_json(hb_buf, sizeof(hb_buf), g_mode);
            if (g_mode == MODE_ONLINE) {
                g_last_broker_ping_ms = xTaskGetTickCount() * portTICK_PERIOD_MS;
                mqtt_client_publish(g_mqtt, TOPIC_HEARTBEAT, hb_buf, strlen(hb_buf), 1, false);
            }
        }

        /* Replay offline buffer when online */
        if (g_mode == MODE_ONLINE && g_offline_buf.count > 0) {
            OfflineMessage_t msg;
            uint8_t replayed = 0;
            while (replayed < 20 && rb_pop(&g_offline_buf, &msg)) {
                mqtt_client_publish(g_mqtt, msg.topic, msg.payload, strlen(msg.payload), msg.qos, false);
                replayed++;
                vTaskDelay(pdMS_TO_TICKS(10));
            }
        }

        vTaskDelay(pdMS_TO_TICKS(500));
    }
}

/**
 * Watchdog Task – detects broker loss, manages operating mode
 */
static void task_watchdog(void *arg)
{
    while (1) {
        uint32_t now     = xTaskGetTickCount() * portTICK_PERIOD_MS;
        uint32_t elapsed = now - g_last_broker_ping_ms;

        /* Emergency stop from event group */
        EventBits_t bits = xEventGroupGetBits(g_events);
        if (bits & EVT_EMERGENCY_STOP) {
            g_mode = MODE_EMERGENCY_STOP;
            apply_control_output(0.0f, 0.0f);
            /* Hard reset after 5 minutes */
            vTaskDelay(pdMS_TO_TICKS(300000));
            esp_restart();
        }

        /* Broker timeout → local autonomy */
        if (g_mode == MODE_ONLINE && elapsed > BROKER_TIMEOUT_MS) {
            g_mode = MODE_LOCAL_AUTONOMY;
        }

        /* Feed hardware watchdog */
        hal_watchdog_feed();

        vTaskDelay(pdMS_TO_TICKS(1000));
    }
}

/**
 * Sensor Task – reads ADC / I²C sensors and posts to queue
 * Runs at 2 Hz from a hardware timer callback.
 */
static void sensor_timer_callback(TimerHandle_t xTimer)
{
    SensorReading_t reading = {0};

    /* Read NTC thermistor via ADC */
    uint16_t adc_raw    = hal_adc_read(ADC_CHANNEL_TEMP);
    reading.temperature = hal_ntc_convert(adc_raw);   /* BSP function */

    /* Read SHT31 humidity sensor via I2C */
    hal_sht31_read(&reading.humidity, NULL);

    /* Read pressure sensor */
    reading.pressure = hal_baro_read_hpa();

    /* Estimate load from compressor current sensor */
    uint16_t current_raw = hal_adc_read(ADC_CHANNEL_CURRENT);
    float    current_a   = current_raw * 0.0049f;  /* 5V ref, 12-bit, 10A/V */
    reading.power_w      = current_a * 220.0f;     /* 220V mains */
    reading.load_pct     = fminf((reading.power_w / 2500.0f) * 100.0f, 100.0f);

    reading.timestamp_ms = xTaskGetTickCount() * portTICK_PERIOD_MS;

    /* Sanity checks */
    if (reading.temperature < TEMP_MIN_SAFE_C || reading.temperature > TEMP_MAX_SAFE_C + 5.0f)
        reading.error_flags |= 0x01;
    if (reading.humidity < 0.0f || reading.humidity > 100.0f)
        reading.error_flags |= 0x02;

    xQueueOverwrite(g_sensor_queue, &reading);
}


/* ═══════════════════════════════════════════════════════════════════════════
 *  Main entry point
 * ═══════════════════════════════════════════════════════════════════════════ */

void app_main(void)
{
    /* Initialize HAL */
    hal_gpio_init();
    hal_adc_init();
    hal_pwm_init();
    hal_i2c_init();
    hal_watchdog_init(10000);   /* 10s HW watchdog */

    /* Create synchronization primitives */
    g_setpoint_mutex    = xSemaphoreCreateMutex();
    g_offline_buf_mutex = xSemaphoreCreateMutex();
    g_sensor_queue      = xQueueCreate(1, sizeof(SensorReading_t));
    g_events            = xEventGroupCreate();

    /* Initialize default command */
    g_command.setpoint      = 22.0f;
    g_command.fan_speed_pct = 50.0f;
    g_command.on            = true;

    /* Sensor timer (2 Hz) */
    TimerHandle_t sensor_timer = xTimerCreate(
        "SensorTimer", pdMS_TO_TICKS(500), pdTRUE, NULL, sensor_timer_callback
    );
    xTimerStart(sensor_timer, 0);

    /* Create tasks */
    xTaskCreatePinnedToCore(task_pid,      "PID",      STACK_PID,      NULL, PRIORITY_PID,      NULL, 0);
    xTaskCreatePinnedToCore(task_mqtt,     "MQTT",     STACK_MQTT,     NULL, PRIORITY_MQTT,     NULL, 1);
    xTaskCreatePinnedToCore(task_watchdog, "Watchdog", STACK_WATCHDOG, NULL, PRIORITY_WATCHDOG, NULL, 1);

    /* FreeRTOS scheduler starts automatically after app_main returns on ESP-IDF */
}
