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
#include <unistd.h>

LOG_MODULE_REGISTER(real_uart_camera_stream, LOG_LEVEL_DBG);

#define MY_WIFI_SSID     "720A"
#define MY_WIFI_PASSWORD "ee720aee720a"
#define SERVER_IP        "192.168.50.33"
#define SERVER_PORT      8080

static struct net_if *iface;
static volatile bool l3_ready = false;
static struct sockaddr_in server_addr_in;
static struct net_mgmt_event_callback wifi_cb;
static struct net_mgmt_event_callback ipv4_cb;

#define UART_DEV_NODE DT_NODELABEL(uart4)
static const struct device *uart_dev;
#define UART_BAUDRATE 460800U

static const struct gpio_dt_spec led = GPIO_DT_SPEC_GET(DT_ALIAS(led1), gpios);

#pragma pack(push, 1)
struct frame_header {
    uint16_t magic_word;
    uint32_t image_size;   // JPEG payload 的 byte 數
    uint16_t box_count;    // 緊接在 header 後面的 bbox_data 筆數（0~10）
};

struct bbox_data {
    uint16_t x;
    uint16_t y;
    uint16_t w;
    uint16_t h;
    float    conf;
    uint8_t  label_char;
};
#pragma pack(pop)

#define HEADER_MAX_BLOCK_SIZE (sizeof(struct frame_header) + 10 * sizeof(struct bbox_data))
#define FRAME_BUF_SIZE 10240  // 與 H7 的 UART_TX_BUF_CAP 對齊

// ==========================================
// 🔧 雙緩衝 + Semaphore（加上 4-byte 對齊）
// ==========================================
#define NUM_RXBUFS 2
static uint8_t frame_buf[NUM_RXBUFS][FRAME_BUF_SIZE] __aligned(4);
static struct k_sem buf_free_sem[NUM_RXBUFS];
static int active_idx = 0;

struct frame_msg {
    int idx;
    uint32_t len;
};
K_MSGQ_DEFINE(frame_ready_msgq, sizeof(struct frame_msg), NUM_RXBUFS, 4);

// ==========================================
// 📡 UART 接收狀態機（在 IRQ context 執行）
// ==========================================
enum parse_state { ST_SYNC1, ST_SYNC2, ST_HEADER, ST_BOXES, ST_PAYLOAD };
static enum parse_state pstate = ST_SYNC1;
static uint32_t pcount;      // 目前這個 state 還需要幾個 byte
static uint8_t *pdst;        // 下一個 byte 要寫去哪
static struct frame_header rx_header;

static void parser_reset_from_error(void) {
    if (pstate == ST_HEADER || pstate == ST_BOXES || pstate == ST_PAYLOAD) {
        k_sem_give(&buf_free_sem[active_idx]);
    }
    pstate = ST_SYNC1;
}

static void parser_feed(uint8_t b) {
    switch (pstate) {
    case ST_SYNC1:
        // 0xAABB 小端序低位元組 0xBB 先出現
        if (b == 0xBB) pstate = ST_SYNC2;
        break;

    case ST_SYNC2:
        if (b == 0xAA) {
            if (k_sem_take(&buf_free_sem[active_idx], K_NO_WAIT) != 0) {
                pstate = ST_SYNC1;
                break;
            }
            frame_buf[active_idx][0] = 0xBB;
            frame_buf[active_idx][1] = 0xAA;
            pdst = &frame_buf[active_idx][2];
            pcount = sizeof(struct frame_header) - 2;
            pstate = ST_HEADER;
        } else if (b != 0xBB) {
            pstate = ST_SYNC1;
        }
        break;

    case ST_HEADER:
        *pdst++ = b;
        pcount--;
        if (pcount == 0) {
            memcpy(&rx_header, frame_buf[active_idx], sizeof(rx_header));
            uint32_t box_bytes = (uint32_t)rx_header.box_count * sizeof(struct bbox_data);
            uint32_t total_needed = sizeof(rx_header) + box_bytes + rx_header.image_size;

            // 確保 image_size 至少 4 bytes（包含 SOI FF D8 與 EOI FF D9）
            if (rx_header.box_count > 10 || rx_header.image_size < 4 ||
                total_needed > FRAME_BUF_SIZE) {
                k_sem_give(&buf_free_sem[active_idx]);
                pstate = ST_SYNC1;
                break;
            }

            if (box_bytes > 0) {
                pcount = box_bytes;
                pstate = ST_BOXES;
            } else {
                pcount = rx_header.image_size;
                pstate = ST_PAYLOAD;
            }
        }
        break;

    case ST_BOXES:
        *pdst++ = b;
        pcount--;
        if (pcount == 0) {
            pcount = rx_header.image_size;
            pstate = ST_PAYLOAD;
        }
        break;

    case ST_PAYLOAD:
        *pdst++ = b;
        pcount--;
        if (pcount == 0) {
            uint32_t box_bytes = (uint32_t)rx_header.box_count * sizeof(struct bbox_data);
            uint32_t jpeg_offset = sizeof(rx_header) + box_bytes;
            uint8_t *jpg = &frame_buf[active_idx][jpeg_offset];
            uint32_t img_sz = rx_header.image_size;

            // 🔧 關鍵修正：驗證 JPEG SOI (FF D8) 與 EOI (FF D9) 標記，攔截漏 byte 或假同步破圖
            if (jpg[0] != 0xFF || jpg[1] != 0xD8 ||
                jpg[img_sz - 2] != 0xFF || jpg[img_sz - 1] != 0xD9) {
                k_sem_give(&buf_free_sem[active_idx]);
                pstate = ST_SYNC1;
                break;
            }

            struct frame_msg m = {
                .idx = active_idx,
                .len = jpeg_offset + img_sz,
            };
            if (k_msgq_put(&frame_ready_msgq, &m, K_NO_WAIT) != 0) {
                k_sem_give(&buf_free_sem[active_idx]);
            }
            active_idx = (active_idx + 1) % NUM_RXBUFS;
            pstate = ST_SYNC1;
        }
        break;
    }
}

static void uart_isr(const struct device *dev, void *user_data) {
    ARG_UNUSED(user_data);
    if (!uart_irq_update(dev)) return;

    // 🔧 檢查硬體是否有 Overrun / Framing Error，若有掉 byte 則立即放棄當前破損幀
    int err = uart_err_check(dev);
    if (err > 0) {
        parser_reset_from_error();
    }

    while (uart_irq_rx_ready(dev)) {
        uint8_t byte;
        int n = uart_fifo_read(dev, &byte, 1);
        if (n != 1) break;
        parser_feed(byte);
    }
}

static void wifi_event_handler(struct net_mgmt_event_callback *cb, uint64_t mgmt_event, struct net_if *iface) {
    ARG_UNUSED(cb);
    ARG_UNUSED(iface);
    if (mgmt_event == NET_EVENT_WIFI_DISCONNECT_RESULT) l3_ready = false;
}

static void ipv4_event_handler(struct net_mgmt_event_callback *cb, uint64_t mgmt_event, struct net_if *iface) {
    ARG_UNUSED(cb);
    ARG_UNUSED(iface);
    if (mgmt_event == NET_EVENT_IPV4_ADDR_ADD) l3_ready = true;
}

int setup_network(void) {
    iface = net_if_get_default();
    net_mgmt_init_event_callback(&wifi_cb, wifi_event_handler, NET_EVENT_WIFI_CONNECT_RESULT | NET_EVENT_WIFI_DISCONNECT_RESULT);
    net_mgmt_add_event_callback(&wifi_cb);
    net_mgmt_init_event_callback(&ipv4_cb, ipv4_event_handler, NET_EVENT_IPV4_ADDR_ADD);
    net_mgmt_add_event_callback(&ipv4_cb);

    struct wifi_connect_req_params wifi_params = {
        .ssid = MY_WIFI_SSID, .ssid_length = strlen(MY_WIFI_SSID),
        .psk = MY_WIFI_PASSWORD, .psk_length = strlen(MY_WIFI_PASSWORD),
        .security = WIFI_SECURITY_TYPE_PSK, .channel = WIFI_CHANNEL_ANY,
    };
    net_mgmt(NET_REQUEST_WIFI_CONNECT, iface, &wifi_params, sizeof(wifi_params));

    while (!l3_ready) k_sleep(K_MSEC(500));
    
    struct zsock_addrinfo *info = NULL;
    if (getaddrinfo(SERVER_IP, NULL, NULL, &info) != 0) return -1;
    memcpy(&server_addr_in, info->ai_addr, sizeof(struct sockaddr_in));
    server_addr_in.sin_port = htons(SERVER_PORT);
    freeaddrinfo(info);
    return 0;
}

int main(void)
{
    int sock = -1;

    gpio_pin_configure_dt(&led, GPIO_OUTPUT_ACTIVE);
    gpio_pin_set_dt(&led, 0);

    uart_dev = DEVICE_DT_GET(UART_DEV_NODE);
    if (!device_is_ready(uart_dev)) {
        LOG_ERR("UART not ready!");
        return 0;
    }
    struct uart_config ucfg = {
        .baudrate = UART_BAUDRATE,
        .parity = UART_CFG_PARITY_NONE,
        .stop_bits = UART_CFG_STOP_BITS_1,
        .data_bits = UART_CFG_DATA_BITS_8,
        .flow_ctrl = UART_CFG_FLOW_CTRL_NONE,
    };
    if (uart_configure(uart_dev, &ucfg) != 0) {
        LOG_ERR("UART configure failed!");
        return 0;
    }

    for (int i = 0; i < NUM_RXBUFS; i++) {
        k_sem_init(&buf_free_sem[i], 1, 1);
    }

    uart_irq_callback_user_data_set(uart_dev, uart_isr, NULL);
    uart_irq_rx_enable(uart_dev);

    LOG_INF("等待 Wi-Fi 連線...");
    if (setup_network() != 0) {
        LOG_ERR("網路初始化失敗！");
        return 0;
    }
    LOG_INF("Wi-Fi 連線成功！UART 接收已啟動，等待 H7 開始傳輸。");

    while (1) {
        if (!l3_ready) {
            if (sock >= 0) { close(sock); sock = -1; }
            k_sleep(K_SECONDS(1));
            continue;
        }

        if (sock < 0) {
            sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if (sock < 0) {
                k_sleep(K_SECONDS(2));
                continue;
            }

            // 🔧 關鍵修正：設定 TCP 傳送逾時 (2 秒)，避免 Wi-Fi 瞬斷或 Server 當機時 send() 永久卡死
            struct zsock_timeval tv = { .tv_sec = 2, .tv_usec = 0 };
            setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

            if (connect(sock, (struct sockaddr *)&server_addr_in, sizeof(server_addr_in)) == 0) {
                LOG_INF("+++ 電腦連線成功！等待 UART frame +++");
            } else {
                close(sock);
                sock = -1;
                k_sleep(K_SECONDS(2));
                continue;
            }
        }

        struct frame_msg m;
        if (k_msgq_get(&frame_ready_msgq, &m, K_MSEC(2000)) != 0) {
            continue;
        }

        gpio_pin_set_dt(&led, 1);

        uint32_t send_start = k_uptime_get_32();
        uint8_t *fb = frame_buf[m.idx];
        uint32_t total_payload = m.len;
        uint32_t sent_bytes = 0;
        int chunk_size = 1460;

        struct frame_header *hdr = (struct frame_header *)fb;
        uint32_t jpeg_offset = sizeof(struct frame_header) + hdr->box_count * sizeof(struct bbox_data);
        uint8_t *jpeg = fb + jpeg_offset;

        if (hdr->image_size >= 4) {
            LOG_INF(
                "L4 JPEG size=%u | HEAD=%02X %02X %02X %02X | "
                "TAIL=%02X %02X %02X %02X",
                hdr->image_size,
                jpeg[0],
                jpeg[1],
                jpeg[2],
                jpeg[3],
                jpeg[hdr->image_size - 4],
                jpeg[hdr->image_size - 3],
                jpeg[hdr->image_size - 2],
                jpeg[hdr->image_size - 1]
            );
        }

        while (sent_bytes < total_payload) {
            int send_len = total_payload - sent_bytes;
            if (send_len > chunk_size) send_len = chunk_size;

            int rc = send(sock, fb + sent_bytes, send_len, 0);
            if (rc <= 0) {
                LOG_ERR("TCP 傳送失敗或逾時斷線 (rc=%d)！", rc);
                close(sock);
                sock = -1;
                break;
            }
            sent_bytes += (uint32_t)rc;
        }
        uint32_t send_duration = k_uptime_get_32() - send_start;

        LOG_INF(" Frame 轉發完成 -> Bytes: %u | TCP 傳輸: %u ms", total_payload, send_duration);
        LOG_INF("RAW: %02X %02X", fb[0], fb[1]);

        // 🔧 關鍵修正：確定所有對 fb 的讀取（包含 LOG_INF）都完成後，才釋放緩衝區給 UART ISR
        k_sem_give(&buf_free_sem[m.idx]);

        gpio_pin_set_dt(&led, 0);
    }
}