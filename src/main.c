
#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>

#include <zephyr/net/net_if.h>
#include <zephyr/net/net_ip.h>
#include <zephyr/net/net_mgmt.h>
#include <zephyr/net/wifi_mgmt.h>
#include <zephyr/net/socket.h>
#include <zephyr/posix/netdb.h>

#include <zephyr/device.h>
#include <zephyr/drivers/uart.h>
#include <zephyr/drivers/gpio.h>

#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <unistd.h>

LOG_MODULE_REGISTER(real_uart_camera_stream, LOG_LEVEL_INF);

/* =========================================================
 * Configuration
 * ========================================================= */

#define MY_WIFI_SSID       "720A"
#define MY_WIFI_PASSWORD   "ee720aee720a"

#define SERVER_IP          "192.168.50.33"
#define SERVER_PORT        8080

#define UART_DEV_NODE      DT_NODELABEL(uart4)
#define UART_BAUDRATE      460800U

#define FIXED_PACKET_SIZE  5000U

#define MAX_DETECTIONS     10

#define NUM_RXBUFS         2

#define UART_RX_CHUNK_SIZE 64

#define TCP_CHUNK_SIZE     1460

#define TCP_SEND_TIMEOUT_S 2

/* =========================================================
 * GPIO
 * ========================================================= */

static const struct gpio_dt_spec led =
    GPIO_DT_SPEC_GET(DT_ALIAS(led1), gpios);

/* =========================================================
 * UART
 * ========================================================= */

static const struct device *uart_dev;

/* =========================================================
 * Wi-Fi / Network
 * ========================================================= */

static struct net_if *iface;

static atomic_t l3_ready = ATOMIC_INIT(0);

static struct sockaddr_in server_addr_in;

static struct net_mgmt_event_callback wifi_cb;
static struct net_mgmt_event_callback ipv4_cb;

/* =========================================================
 * Packet protocol
 * Must match STM32H747
 * ========================================================= */

#pragma pack(push, 1)

struct frame_header {
    uint16_t magic_word;
    uint32_t image_size;
    uint16_t box_count;
};

struct bbox_data {
    uint16_t x;
    uint16_t y;
    uint16_t w;
    uint16_t h;
    float conf;
    uint8_t label_char;
};

#pragma pack(pop)

#define HEADER_SIZE       sizeof(struct frame_header)
#define BBOX_SIZE         sizeof(struct bbox_data)

#define MAX_PACKET_SIZE   FIXED_PACKET_SIZE

#define MAX_HEADER_SIZE \
    (HEADER_SIZE + MAX_DETECTIONS * BBOX_SIZE)

BUILD_ASSERT(sizeof(struct frame_header) == 8,
             "frame_header size must be 8 bytes");

BUILD_ASSERT(sizeof(struct bbox_data) == 13,
             "bbox_data size must be 13 bytes");

/* =========================================================
 * RX double buffers
 * ========================================================= */

static uint8_t frame_buf[NUM_RXBUFS][FIXED_PACKET_SIZE]
    __aligned(32);

static struct k_sem buf_free_sem[NUM_RXBUFS];

static int active_idx = 0;

/*
 * rx_idx:
 * The buffer currently owned by UART parser.
 *
 * active_idx:
 * The next buffer to acquire when a new frame starts.
 */

static int rx_idx = -1;

static bool rx_buf_owned = false;

/* =========================================================
 * Frame message queue
 * ========================================================= */

struct frame_msg {
    int idx;
    uint32_t len;
};

K_MSGQ_DEFINE(
    frame_ready_msgq,
    sizeof(struct frame_msg),
    NUM_RXBUFS,
    4
);

/* =========================================================
 * Parser
 * ========================================================= */

enum parse_state {
    ST_SYNC1,
    ST_SYNC2,
    ST_HEADER,
    ST_BOXES,
    ST_PAYLOAD
};

static enum parse_state pstate = ST_SYNC1;

static uint32_t pcount;

static uint8_t *pdst;

static struct frame_header rx_header;

/* =========================================================
 * Parser statistics
 * ========================================================= */

static volatile uint32_t rx_frames = 0;
static volatile uint32_t rx_errors = 0;
static volatile uint32_t rx_dropped = 0;

/* =========================================================
 * Parser reset
 * ========================================================= */

static void parser_reset(void)
{
    if (rx_buf_owned && rx_idx >= 0) {

        k_sem_give(&buf_free_sem[rx_idx]);
    }

    rx_buf_owned = false;
    rx_idx = -1;

    pstate = ST_SYNC1;
    pcount = 0;
    pdst = NULL;
}

/* =========================================================
 * Parser feed
 * Called from UART ISR
 * ========================================================= */

static void parser_feed(uint8_t b)
{
    switch (pstate) {

    /* -----------------------------------------------------
     * Find first magic byte: 0xBB
     * ----------------------------------------------------- */

    case ST_SYNC1:

        if (b == 0xBB) {
            pstate = ST_SYNC2;
        }

        break;

    /* -----------------------------------------------------
     * Find second magic byte: 0xAA
     * ----------------------------------------------------- */

    case ST_SYNC2:

        if (b == 0xAA) {

            /*
             * Acquire one free RX buffer.
             */

            if (k_sem_take(
                    &buf_free_sem[active_idx],
                    K_NO_WAIT
                ) != 0) {

                rx_dropped++;

                pstate = ST_SYNC1;
                break;
            }

            rx_idx = active_idx;

            rx_buf_owned = true;

            frame_buf[rx_idx][0] = 0xBB;
            frame_buf[rx_idx][1] = 0xAA;

            pdst = &frame_buf[rx_idx][2];

            pcount = sizeof(struct frame_header) - 2;

            pstate = ST_HEADER;

        } else if (b == 0xBB) {

            /*
             * Overlapping synchronization:
             * BB BB AA -> use the second BB.
             */

            pstate = ST_SYNC2;

        } else {

            pstate = ST_SYNC1;
        }

        break;

    /* -----------------------------------------------------
     * Receive remaining header
     * ----------------------------------------------------- */

    case ST_HEADER:

        *pdst++ = b;

        pcount--;

        if (pcount == 0) {

            memcpy(
                &rx_header,
                frame_buf[rx_idx],
                sizeof(rx_header)
            );

            /*
             * Validate magic word.
             */

            if (rx_header.magic_word != 0xAABB) {

                rx_errors++;

                parser_reset();
                break;
            }

            /*
             * Validate bounding box count.
             */

            if (rx_header.box_count > MAX_DETECTIONS) {

                rx_errors++;

                parser_reset();
                break;
            }

            /*
             * Validate JPEG size.
             */

            if (rx_header.image_size < 4) {

                rx_errors++;

                parser_reset();
                break;
            }

            uint32_t box_bytes =
                (uint32_t)rx_header.box_count * BBOX_SIZE;

            uint32_t total_needed =
                HEADER_SIZE +
                box_bytes +
                rx_header.image_size;

            /*
             * H7 sends a fixed 4500-byte packet.
             *
             * The actual useful data must fit inside it.
             */

            if (total_needed > FIXED_PACKET_SIZE) {

                rx_errors++;

                parser_reset();
                break;
            }

            /*
             * Start receiving bounding boxes or JPEG.
             */

            if (box_bytes > 0) {

                pcount = box_bytes;

                pstate = ST_BOXES;

            } else {

                pcount = rx_header.image_size;

                pstate = ST_PAYLOAD;
            }
        }

        break;

    /* -----------------------------------------------------
     * Receive bounding boxes
     * ----------------------------------------------------- */

    case ST_BOXES:

        *pdst++ = b;

        pcount--;

        if (pcount == 0) {

            pcount = rx_header.image_size;

            pstate = ST_PAYLOAD;
        }

        break;

    /* -----------------------------------------------------
     * Receive JPEG payload
     * ----------------------------------------------------- */

    case ST_PAYLOAD:

        *pdst++ = b;

        pcount--;

        if (pcount == 0) {

            uint32_t box_bytes =
                (uint32_t)rx_header.box_count * BBOX_SIZE;

            uint32_t jpeg_offset =
                HEADER_SIZE + box_bytes;

            uint8_t *jpg =
                &frame_buf[rx_idx][jpeg_offset];

            uint32_t img_sz =
                rx_header.image_size;

            /*
             * JPEG SOI: FF D8
             * JPEG EOI: FF D9
             */

            if (jpg[0] != 0xFF ||
                jpg[1] != 0xD8 ||
                jpg[img_sz - 2] != 0xFF ||
                jpg[img_sz - 1] != 0xD9) {

                rx_errors++;

                parser_reset();
                break;
            }

            /*
             * Actual useful frame length:
             *
             * Header + BBoxes + JPEG
             *
             * Padding is not forwarded to PC.
             */

            struct frame_msg m = {
                .idx = rx_idx,
                .len = jpeg_offset + img_sz
            };

            if (k_msgq_put(
                    &frame_ready_msgq,
                    &m,
                    K_NO_WAIT
                ) != 0) {

                /*
                 * Queue full:
                 * Release this buffer.
                 */

                k_sem_give(&buf_free_sem[rx_idx]);

                rx_dropped++;

            } else {

                rx_frames++;
            }

            /*
             * Release parser ownership.
             * The queued buffer remains unavailable
             * until the TCP thread finishes using it.
             */

            active_idx = (rx_idx + 1) % NUM_RXBUFS;

            rx_idx = -1;
            rx_buf_owned = false;

            pstate = ST_SYNC1;
            pcount = 0;
            pdst = NULL;
        }

        break;
    }
}

/* =========================================================
 * UART ISR
 * ========================================================= */

static void uart_isr(
    const struct device *dev,
    void *user_data
)
{
    ARG_UNUSED(user_data);

    if (!uart_irq_update(dev)) {
        return;
    }

    /*
     * Check UART hardware errors.
     */

    int err = uart_err_check(dev);

    if (err > 0) {

        rx_errors++;

        parser_reset();
    }

    /*
     * Read multiple bytes per FIFO operation.
     *
     * This reduces ISR overhead compared with
     * reading one byte per uart_fifo_read().
     */

    uint8_t rx_data[UART_RX_CHUNK_SIZE];

    while (uart_irq_rx_ready(dev)) {

        int n = uart_fifo_read(
            dev,
            rx_data,
            sizeof(rx_data)
        );

        if (n <= 0) {
            break;
        }

        for (int i = 0; i < n; i++) {

            parser_feed(rx_data[i]);
        }
    }
}

/* =========================================================
 * Wi-Fi events
 * ========================================================= */

static void wifi_event_handler(
    struct net_mgmt_event_callback *cb,
    uint64_t mgmt_event,
    struct net_if *iface
)
{
    ARG_UNUSED(cb);
    ARG_UNUSED(iface);

    if (mgmt_event == NET_EVENT_WIFI_DISCONNECT_RESULT) {

        atomic_set(&l3_ready, 0);

        LOG_WRN("Wi-Fi disconnected");
    }
}

/* =========================================================
 * IPv4 events
 * ========================================================= */

static void ipv4_event_handler(
    struct net_mgmt_event_callback *cb,
    uint64_t mgmt_event,
    struct net_if *iface
)
{
    ARG_UNUSED(cb);
    ARG_UNUSED(iface);

    if (mgmt_event == NET_EVENT_IPV4_ADDR_ADD) {

        atomic_set(&l3_ready, 1);

        LOG_INF("IPv4 address acquired");
    }
}

/* =========================================================
 * Network initialization
 * ========================================================= */

static int setup_network(void)
{
    iface = net_if_get_default();

    if (iface == NULL) {

        LOG_ERR("No default network interface");

        return -1;
    }

    net_mgmt_init_event_callback(
        &wifi_cb,
        wifi_event_handler,
        NET_EVENT_WIFI_CONNECT_RESULT |
        NET_EVENT_WIFI_DISCONNECT_RESULT
    );

    net_mgmt_add_event_callback(&wifi_cb);

    net_mgmt_init_event_callback(
        &ipv4_cb,
        ipv4_event_handler,
        NET_EVENT_IPV4_ADDR_ADD
    );

    net_mgmt_add_event_callback(&ipv4_cb);

    struct wifi_connect_req_params wifi_params = {
        .ssid = MY_WIFI_SSID,
        .ssid_length = strlen(MY_WIFI_SSID),

        .psk = MY_WIFI_PASSWORD,
        .psk_length = strlen(MY_WIFI_PASSWORD),

        .security = WIFI_SECURITY_TYPE_PSK,
        .channel = WIFI_CHANNEL_ANY
    };

    int ret = net_mgmt(
        NET_REQUEST_WIFI_CONNECT,
        iface,
        &wifi_params,
        sizeof(wifi_params)
    );

    if (ret != 0) {

        LOG_ERR("Wi-Fi connect request failed: %d", ret);

        return -1;
    }

    LOG_INF("Waiting for Wi-Fi IPv4...");

    while (!atomic_get(&l3_ready)) {

        k_sleep(K_MSEC(500));
    }

    /*
     * Resolve server address.
     */

    struct zsock_addrinfo *info = NULL;

    ret = getaddrinfo(
        SERVER_IP,
        NULL,
        NULL,
        &info
    );

    if (ret != 0 || info == NULL) {

        LOG_ERR("getaddrinfo failed: %d", ret);

        return -1;
    }

    memcpy(
        &server_addr_in,
        info->ai_addr,
        sizeof(server_addr_in)
    );

    server_addr_in.sin_family = AF_INET;
    server_addr_in.sin_port = htons(SERVER_PORT);

    freeaddrinfo(info);

    LOG_INF(
        "Server: %s:%u",
        SERVER_IP,
        SERVER_PORT
    );

    return 0;
}

/* =========================================================
 * TCP socket creation
 * ========================================================= */

static int create_tcp_connection(void)
{
    int sock = socket(
        AF_INET,
        SOCK_STREAM,
        IPPROTO_TCP
    );

    if (sock < 0) {

        LOG_ERR("Socket creation failed: %d", errno);

        return -1;
    }

    /*
     * Send timeout.
     */

    struct zsock_timeval tv = {
        .tv_sec = TCP_SEND_TIMEOUT_S,
        .tv_usec = 0
    };

    setsockopt(
        sock,
        SOL_SOCKET,
        SO_SNDTIMEO,
        &tv,
        sizeof(tv)
    );

    /*
     * Connect to PC.
     */

    int ret = connect(
        sock,
        (struct sockaddr *)&server_addr_in,
        sizeof(server_addr_in)
    );

    if (ret < 0) {

        LOG_ERR("TCP connect failed: %d", errno);

        close(sock);

        return -1;
    }

    LOG_INF("PC TCP connection established");

    return sock;
}

/* =========================================================
 * TCP send all
 * ========================================================= */

static int tcp_send_all(
    int sock,
    const uint8_t *data,
    uint32_t len
)
{
    uint32_t sent = 0;

    while (sent < len) {

        uint32_t remaining = len - sent;

        uint32_t send_len =
            remaining > TCP_CHUNK_SIZE
            ? TCP_CHUNK_SIZE
            : remaining;

        int ret = send(
            sock,
            data + sent,
            send_len,
            0
        );

        if (ret < 0) {

            LOG_ERR(
                "TCP send error: errno=%d",
                errno
            );

            return -1;
        }

        if (ret == 0) {

            LOG_ERR("TCP connection closed");

            return -1;
        }

        sent += (uint32_t)ret;
    }

    return (int)sent;
}

/* =========================================================
 * Main
 * ========================================================= */

int main(void)
{
    int sock = -1;

    /* -----------------------------------------------------
     * LED
     * ----------------------------------------------------- */

    if (gpio_is_ready_dt(&led)) {

        gpio_pin_configure_dt(
            &led,
            GPIO_OUTPUT_INACTIVE
        );
    }

    /* -----------------------------------------------------
     * UART initialization
     * ----------------------------------------------------- */

    uart_dev = DEVICE_DT_GET(UART_DEV_NODE);

    if (!device_is_ready(uart_dev)) {

        LOG_ERR("UART4 not ready");

        return 0;
    }

    struct uart_config ucfg = {
        .baudrate = UART_BAUDRATE,
        .parity = UART_CFG_PARITY_NONE,
        .stop_bits = UART_CFG_STOP_BITS_1,
        .data_bits = UART_CFG_DATA_BITS_8,
        .flow_ctrl = UART_CFG_FLOW_CTRL_NONE
    };

    if (uart_configure(uart_dev, &ucfg) != 0) {

        LOG_ERR("UART configure failed");

        return 0;
    }

    /* -----------------------------------------------------
     * RX buffer semaphores
     * ----------------------------------------------------- */

    for (int i = 0; i < NUM_RXBUFS; i++) {

        k_sem_init(
            &buf_free_sem[i],
            1,
            1
        );
    }

    /* -----------------------------------------------------
     * UART interrupt callback
     * ----------------------------------------------------- */

    uart_irq_callback_user_data_set(
        uart_dev,
        uart_isr,
        NULL
    );

    uart_irq_rx_enable(uart_dev);

    LOG_INF("================================");
    LOG_INF("STM32L4 UART Camera Receiver");
    LOG_INF("UART = %u", UART_BAUDRATE);
    LOG_INF("Packet = %u Bytes", FIXED_PACKET_SIZE);
    LOG_INF("RX = Interrupt / No DMA");
    LOG_INF("RX Buffers = %u", NUM_RXBUFS);
    LOG_INF("================================");

    /* -----------------------------------------------------
     * Wi-Fi
     * ----------------------------------------------------- */

    LOG_INF("Connecting Wi-Fi...");

    if (setup_network() != 0) {

        LOG_ERR("Network initialization failed");

        return 0;
    }

    LOG_INF("Wi-Fi ready");

    /* -----------------------------------------------------
     * TCP / Frame forwarding
     * ----------------------------------------------------- */

    uint32_t forwarded_frames = 0;

    while (1) {

        /*
         * Reconnect if Wi-Fi is disconnected.
         */

        if (!atomic_get(&l3_ready)) {

            if (sock >= 0) {

                close(sock);

                sock = -1;
            }

            k_sleep(K_SECONDS(1));

            continue;
        }

        /*
         * Create TCP connection when needed.
         */

        if (sock < 0) {

            sock = create_tcp_connection();

            if (sock < 0) {

                k_sleep(K_SECONDS(2));

                continue;
            }
        }

        /*
         * Wait for one complete UART frame.
         */

        struct frame_msg m;

        if (k_msgq_get(
                &frame_ready_msgq,
                &m,
                K_MSEC(1000)
            ) != 0) {

            continue;
        }

        gpio_pin_set_dt(&led, 1);

        uint8_t *fb = frame_buf[m.idx];

        uint32_t total_payload = m.len;

        struct frame_header *hdr =
            (struct frame_header *)fb;

        uint32_t jpeg_offset =
            HEADER_SIZE +
            (uint32_t)hdr->box_count * BBOX_SIZE;

        uint8_t *jpeg =
            fb + jpeg_offset;

        /*
         * Log frame information.
         */

        LOG_INF(
            "[RX] JPEG=%u Boxes=%u Total=%u",
            hdr->image_size,
            hdr->box_count,
            total_payload
        );

        LOG_DBG(
            "[JPEG] HEAD=%02X %02X TAIL=%02X %02X",
            jpeg[0],
            jpeg[1],
            jpeg[hdr->image_size - 2],
            jpeg[hdr->image_size - 1]
        );

        /*
         * TCP transmission.
         */

        uint32_t send_start = k_uptime_get_32();

        int sent = tcp_send_all(
            sock,
            fb,
            total_payload
        );

        uint32_t send_duration =
            k_uptime_get_32() - send_start;

        if (sent < 0) {

            LOG_ERR("TCP forwarding failed");

            close(sock);

            sock = -1;

        } else {

            forwarded_frames++;

            LOG_INF(
                "[TCP] Frame=%u Bytes=%u Time=%u ms",
                forwarded_frames,
                total_payload,
                send_duration
            );
        }

        /*
         * Release RX buffer only after all TCP reads finish.
         */

        k_sem_give(&buf_free_sem[m.idx]);

        gpio_pin_set_dt(&led, 0);

        /*
         * Periodic receiver statistics.
         */

        if ((forwarded_frames % 10) == 0) {

            LOG_INF(
                "[RX STATS] Received=%u Errors=%u Dropped=%u",
                (unsigned)rx_frames,
                (unsigned)rx_errors,
                (unsigned)rx_dropped
            );
        }
    }

    return 0;
}
