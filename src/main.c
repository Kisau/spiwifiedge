/*
 * Copyright (c) 2024, Nordic Semiconductor
 * SPDX-License-Identifier: Apache-2.0
 */

#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <zephyr/net/net_if.h>
#include <zephyr/net/net_ip.h>
#include <zephyr/net/net_mgmt.h>
#include <zephyr/net/wifi_mgmt.h>
#include <zephyr/net/socket.h>
#include <zephyr/posix/netdb.h>
#include <zephyr/device.h>
#include <zephyr/drivers/spi.h>
#include <zephyr/drivers/gpio.h>
#include <unistd.h>

LOG_MODULE_REGISTER(real_spi_camera_stream, LOG_LEVEL_DBG);

/* ================= 網路設定區 ================= */
#define MY_WIFI_SSID     "fish008"
#define MY_WIFI_PASSWORD "84026145"
#define SERVER_IP        "192.168.50.236"
#define SERVER_PORT      8080

static struct net_if *iface;
static bool l3_ready = false;
static struct sockaddr_in server_addr_in;
static struct net_mgmt_event_callback wifi_cb;
static struct net_mgmt_event_callback ipv4_cb;

/* ================= 硬體設定區 (SPI & GPIO) ================= */
#define SPI_DEV DEVICE_DT_GET(DT_NODELABEL(spi1))
static const struct gpio_dt_spec led = GPIO_DT_SPEC_GET(DT_ALIAS(led1), gpios);
static const struct gpio_dt_spec nss = {
    .port = DEVICE_DT_GET(DT_NODELABEL(gpioa)),
    .pin = 2,
    .dt_flags = GPIO_ACTIVE_LOW,
};

#define FRAME_BUF_SIZE 100000
static uint8_t frame_buf[FRAME_BUF_SIZE];

/* ================= 資料結構 ================= */
#pragma pack(push, 1)
struct spi_header {
    uint16_t magic_word;
    uint32_t image_size;
    uint16_t box_count;  
};
#pragma pack(pop)

/* --- 網路事件處理 --- */
static void wifi_event_handler(struct net_mgmt_event_callback *cb, uint64_t mgmt_event, struct net_if *iface) {
    if (mgmt_event == NET_EVENT_WIFI_DISCONNECT_RESULT) l3_ready = false;
}
static void ipv4_event_handler(struct net_mgmt_event_callback *cb, uint64_t mgmt_event, struct net_if *iface) {
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

static void clear_fifo(void) {
    SPI_TypeDef *spi = SPI1;
    for (int i = 0; i < 10; i++) {
        if (spi->SR & SPI_SR_RXNE) { (void)spi->DR; }
    }
}

int main(void)
{
    int sock = -1;

    gpio_pin_configure_dt(&led, GPIO_OUTPUT_ACTIVE);
    gpio_pin_set_dt(&led, 0);
    gpio_pin_configure_dt(&nss, GPIO_INPUT | GPIO_PULL_UP);

    LOG_INF("等待 Wi-Fi 連線...");
    setup_network();
    LOG_INF("Wi-Fi 連線成功！開始監聽真實 STM32H7 相機 SPI 訊號...");

    const struct spi_config spi_cfg = {
        .frequency = 1562500 ,
        .operation = SPI_OP_MODE_SLAVE | SPI_WORD_SET(8) | SPI_LINES_SINGLE | SPI_TRANSFER_MSB,
        .slave = 0, .cs = NULL,
    };

    while (1) {
        if (!l3_ready) {
            if (sock >= 0) { close(sock); sock = -1; }
            k_sleep(K_SECONDS(1)); continue;
        }

        if (sock < 0) {
            sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if (connect(sock, (struct sockaddr *)&server_addr_in, sizeof(server_addr_in)) == 0) {
                
                LOG_INF("+++ 電腦連線成功！等待 H7 拉低 NSS +++");
            } else {
                close(sock); sock = -1; k_sleep(K_SECONDS(2)); continue;
            }
        }

        if (gpio_pin_get_dt(&nss) == 0) {
            gpio_pin_set_dt(&led, 1);
            clear_fifo();

            struct spi_buf rx_hdr = {.buf = frame_buf, .len = sizeof(struct spi_header)};
            struct spi_buf_set rx_hdr_set = {.buffers = &rx_hdr, .count = 1};
            
            spi_transceive(SPI_DEV, &spi_cfg, NULL, &rx_hdr_set);
            
            struct spi_header *header = (struct spi_header *)frame_buf;

            if (header->magic_word == 0xAABB) {
                
                uint32_t remaining_bytes = (header->box_count * 11) + header->image_size;
                
                if (remaining_bytes > 0 && (remaining_bytes + 8) <= FRAME_BUF_SIZE) {
                    
                    /* 接完剩下的 */
                    struct spi_buf rx_data = {.buf = frame_buf + 8, .len = remaining_bytes};
                    struct spi_buf_set rx_data_set = {.buffers = &rx_data, .count = 1};
                    spi_transceive(SPI_DEV, &spi_cfg, NULL, &rx_data_set);


                    /* 穩定分塊透過 TCP 送給電腦 */
                    uint32_t total_payload = 8 + remaining_bytes;
                    uint32_t sent_bytes = 0;
                    int chunk_size = 1460; 
                    uint32_t wifi_start_time = k_uptime_get_32();
                    while (sent_bytes < total_payload) {
                        int send_len = total_payload - sent_bytes;
                        if (send_len > chunk_size) send_len = chunk_size;

                        int rc = send(sock, frame_buf + sent_bytes, send_len, 0);
                        if (rc < 0) {
                            LOG_ERR("TCP 斷線！");
                            close(sock); sock = -1;
                            break;
                        }
                        sent_bytes += rc; 
                    }
                    uint32_t wifi_end_time = k_uptime_get_32();
                    uint32_t wifi_duration = wifi_end_time - wifi_start_time;
                    LOG_INF("✅ 單幀實機影像接收與轉發成功！(Wi-Fi TCP 發送耗時: %u 毫秒)", wifi_duration);
                }
            } else {
                LOG_WRN("雜訊干擾，Magic Word 錯誤: 0x%04X", header->magic_word);
            }

            gpio_pin_set_dt(&led, 0);

            while (gpio_pin_get_dt(&nss) == 0) {
                k_sleep(K_MSEC(1));
            }
        }
        k_sleep(K_MSEC(2));
    }
}