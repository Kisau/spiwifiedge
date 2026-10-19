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
#include <zephyr/sys/ring_buffer.h>
#include <zephyr/sys/atomic.h>

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

LOG_MODULE_REGISTER(real_uart_camera_stream, LOG_LEVEL_INF);


/* ============================================================
 * 1. Network configuration
 * ============================================================ */

#define MY_WIFI_SSID       "720A"
#define MY_WIFI_PASSWORD   "ee720aee720a"

#define SERVER_IP          "192.168.50.33"
#define SERVER_PORT        8080

static struct net_if *iface;
static volatile bool l3_ready = false;

static struct sockaddr_in server_addr_in;
static struct net_mgmt_event_callback wifi_cb;
static struct net_mgmt_event_callback ipv4_cb;


/* ============================================================
 * 2. UART configuration
 * ============================================================ */

#define UART_DEV_NODE      DT_NODELABEL(uart4)
#define UART_BAUDRATE      460800U

/*
 * Async UART RX timeout, in microseconds.
 *
 * A timeout lets the driver report partially filled DMA buffers
 * when the incoming stream pauses.
 */
#define UART_RX_TIMEOUT_US 2000


static const struct device *uart_dev;

static const struct gpio_dt_spec led =
    GPIO_DT_SPEC_GET(DT_ALIAS(led1), gpios);


/* ============================================================
 * 3. H7 -> L4 frame protocol
 *
 * The protocol remains compatible with the original main branch:
 *
 *   frame_header
 *   bbox_data[box_count]
 *   JPEG[image_size]
 *
 * STM32 is little-endian. The transmitted magic bytes are BB AA,
 * corresponding to magic_word == 0xAABB on the L4.
 * ============================================================ */

#pragma pack(push, 1)

struct frame_header {
    uint16_t magic_word;
    uint32_t image_size;   /* JPEG payload length, in bytes */
    uint16_t box_count;    /* Number of bbox_data records */
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


BUILD_ASSERT(sizeof(struct frame_header) == 8,
             "Unexpected frame_header size");

BUILD_ASSERT(sizeof(struct bbox_data) == 13,
             "Unexpected bbox_data size");


#define MAX_BOX_COUNT          10U

/*
 * RAM-conscious configuration:
 *
 * Two 16 KB frame buffers = 32 KB total.
 * The size includes the frame header, bounding boxes and JPEG.
 *
 * This supports JPEGs around 11 KB and leaves space for metadata.
 */
#define FRAME_BUF_SIZE         (12U * 1024U)

/*
 * Maximum plausible JPEG payload accepted by the parser when
 * determining how many bytes to skip after a frame is dropped.
 *
 * A frame larger than FRAME_BUF_SIZE will be discarded, but its
 * declared body can still be skipped to preserve synchronization.
 */
#define MAX_WIRE_IMAGE_SIZE    (20U * 1024U)


/* ============================================================
 * 4. Frame buffers and queue
 * ============================================================ */

#define NUM_FRAME_BUFS  2

static uint8_t frame_buf[NUM_FRAME_BUFS][FRAME_BUF_SIZE]
    __aligned(4);

static struct k_sem buf_free_sem[NUM_FRAME_BUFS];

static int active_idx = 0;

struct frame_msg {
    int      idx;
    uint32_t len;
};

/*
 * At most two completed frames can be waiting for or undergoing
 * TCP transmission, because there are only two frame buffers.
 */
K_MSGQ_DEFINE(frame_ready_msgq,
              sizeof(struct frame_msg),
              NUM_FRAME_BUFS,
              4);


/* ============================================================
 * 5. UART DMA buffers and Ring Buffer
 * ============================================================ */

/*
 * The Ring Buffer holds UART bytes that have not yet been consumed
 * by the parser thread. It is not a frame buffer.
 *
 * Keep this moderate to reduce STM32L4 RAM consumption.
 */
#define UART_RING_SIZE  4096U

RING_BUF_DECLARE(uart_rb, UART_RING_SIZE);

K_SEM_DEFINE(rb_sem, 0, 1);


/*
 * Three DMA buffers are managed as a pool.
 *
 * A buffer is not reused until UART_RX_BUF_RELEASED confirms that
 * the UART driver no longer owns it.
 */
#define DMA_BUF_COUNT   3
#define DMA_BUF_SIZE    1024U

static uint8_t dma_rx_buf[DMA_BUF_COUNT][DMA_BUF_SIZE]
    __aligned(4);

static bool dma_buf_in_use[DMA_BUF_COUNT];

static struct k_spinlock dma_pool_lock;


/*
 * Set when received bytes have been lost or UART RX has stopped.
 *
 * The UART callback requests resynchronization; only the parser
 * thread resets parser state and releases an active frame buffer.
 */
static atomic_t rx_stream_discontinuity;


/* ============================================================
 * 6. Parser thread
 * ============================================================ */

#define PARSER_STACK_SIZE  2048
#define PARSER_PRIORITY    4

enum parse_state {
    ST_SYNC1,
    ST_SYNC2,
    ST_HEADER,
    ST_BOXES,
    ST_PAYLOAD,

    /* No frame buffer was available; collect only the header. */
    ST_DROP_HEADER,

    /* Discard the declared body of a frame without storing it. */
    ST_DROP_BODY
};


/*
 * Parser state is accessed only by parser_thread().
 * The UART callback never modifies these variables.
 */
static enum parse_state pstate = ST_SYNC1;

static uint32_t pcount;
static uint8_t *pdst;

static struct frame_header rx_header;

/*
 * True when the parser owns buf_free_sem[active_idx] for an
 * unfinished frame.
 */
static bool frame_owned;

/*
 * Scratch storage used when no frame buffer is available.
 */
static uint8_t drop_header[sizeof(struct frame_header)];
static uint32_t drop_header_pos;


/* ============================================================
 * 7. DMA buffer pool helpers
 * ============================================================ */

static int dma_buffer_reserve(void)
{
    int idx = -1;
    k_spinlock_key_t key = k_spin_lock(&dma_pool_lock);

    for (int i = 0; i < DMA_BUF_COUNT; i++) {
        if (!dma_buf_in_use[i]) {
            dma_buf_in_use[i] = true;
            idx = i;
            break;
        }
    }

    k_spin_unlock(&dma_pool_lock, key);

    return idx;
}


static void dma_buffer_release_index(int idx)
{
    if (idx < 0 || idx >= DMA_BUF_COUNT) {
        return;
    }

    k_spinlock_key_t key = k_spin_lock(&dma_pool_lock);
    dma_buf_in_use[idx] = false;
    k_spin_unlock(&dma_pool_lock, key);
}


static int dma_buffer_find_index(const uint8_t *buf)
{
    for (int i = 0; i < DMA_BUF_COUNT; i++) {
        if (buf == dma_rx_buf[i]) {
            return i;
        }
    }

    return -1;
}


static void dma_buffer_release_ptr(const uint8_t *buf)
{
    int idx = dma_buffer_find_index(buf);

    if (idx < 0) {
        LOG_WRN("Unknown released DMA buffer");
        return;
    }

    dma_buffer_release_index(idx);
}


/*
 * Start a new asynchronous UART RX operation using a free DMA
 * buffer. The helper can be used during initialization and recovery.
 */
static int uart_rx_start_with_free_buffer(const struct device *dev)
{
    int idx = dma_buffer_reserve();

    if (idx < 0) {
        return -ENOMEM;
    }

    int rc = uart_rx_enable(dev,
                            dma_rx_buf[idx],
                            DMA_BUF_SIZE,
                            UART_RX_TIMEOUT_US);

    if (rc != 0) {
        dma_buffer_release_index(idx);
        LOG_ERR("uart_rx_enable failed: %d", rc);
    }

    return rc;
}


/* ============================================================
 * 8. Parser reset and recovery
 * ============================================================ */

static void parser_release_owned_buffer(void)
{
    if (frame_owned) {
        k_sem_give(&buf_free_sem[active_idx]);
        frame_owned = false;
    }
}


static void parser_to_sync(void)
{
    pstate = ST_SYNC1;
    pcount = 0;
    pdst = NULL;
    drop_header_pos = 0;
}


/*
 * Called only by parser_thread().
 */
static void parser_reset_from_error(void)
{
    parser_release_owned_buffer();
    parser_to_sync();
}


/*
 * Called by the UART callback after byte loss or a stopped receiver.
 *
 * Use compare-and-set so repeated events do not continually change
 * a discontinuity flag that the parser thread is already handling.
 */
static void request_stream_resync(void)
{
    if (atomic_cas(&rx_stream_discontinuity, 0, 1)) {
        k_sem_give(&rb_sem);
    }
}


/*
 * Recover from a gap in the byte stream.
 *
 * While rx_stream_discontinuity is set, the UART callback discards
 * incoming RX chunks instead of adding new bytes to the Ring Buffer.
 * This lets the parser thread flush the buffer without racing a
 * second producer write.
 */
static void parser_handle_discontinuity(void)
{
    uint8_t discard[128];

    parser_reset_from_error();

    while (ring_buf_get(&uart_rb,
                        discard,
                        sizeof(discard)) > 0) {
        /* Drop all bytes queued before the discontinuity. */
    }

    atomic_clear(&rx_stream_discontinuity);

    LOG_WRN("UART stream discontinuity; parser reset");
}


/* ============================================================
 * 9. Header validation
 * ============================================================ */

/*
 * Decode and validate the common protocol header.
 *
 * This checks the declared wire format. Whether the frame fits into
 * our allocated Frame Buffer is checked separately.
 */
static bool decode_frame_header(const uint8_t *raw,
                                struct frame_header *hdr,
                                uint32_t *box_bytes,
                                uint32_t *body_bytes)
{
    memcpy(hdr, raw, sizeof(*hdr));

    if (hdr->magic_word != 0xAABB) {
        return false;
    }

    if (hdr->box_count > MAX_BOX_COUNT) {
        return false;
    }

    if (hdr->image_size < 4U ||
        hdr->image_size > MAX_WIRE_IMAGE_SIZE) {
        return false;
    }

    *box_bytes = (uint32_t)hdr->box_count *
                 (uint32_t)sizeof(struct bbox_data);

    *body_bytes = *box_bytes + hdr->image_size;

    return true;
}


/*
 * Choose any available frame buffer, starting from active_idx.
 *
 * This avoids repeatedly dropping frames because active_idx happens
 * to refer to a buffer that is still being sent over TCP.
 */
static bool parser_acquire_frame_buffer(void)
{
    for (int n = 0; n < NUM_FRAME_BUFS; n++) {
        int candidate = (active_idx + n) % NUM_FRAME_BUFS;

        if (k_sem_take(&buf_free_sem[candidate], K_NO_WAIT) == 0) {
            active_idx = candidate;
            frame_owned = true;
            return true;
        }
    }

    return false;
}


/* ============================================================
 * 10. Byte-by-byte packet parser
 *
 * Important:
 * - Frame boundaries come from the H7 header's image_size.
 * - JPEG FF D9 is validated at the declared end of the JPEG.
 * - We do not terminate a frame merely because FF D9 appears.
 * ============================================================ */

static void parser_feed(uint8_t b)
{
    switch (pstate) {

    case ST_SYNC1:
        if (b == 0xBB) {
            pstate = ST_SYNC2;
        }
        break;


    case ST_SYNC2:
        if (b == 0xAA) {

            if (parser_acquire_frame_buffer()) {

                frame_buf[active_idx][0] = 0xBB;
                frame_buf[active_idx][1] = 0xAA;

                pdst = &frame_buf[active_idx][2];
                pcount = sizeof(struct frame_header) - 2U;

                pstate = ST_HEADER;

            } else {
                /*
                 * Both frame buffers are busy.
                 *
                 * Still read the header so we can skip the rest of
                 * this frame based on its declared length, rather
                 * than scanning arbitrary JPEG bytes for BB AA.
                 */
                drop_header[0] = 0xBB;
                drop_header[1] = 0xAA;

                drop_header_pos = 2U;
                pstate = ST_DROP_HEADER;
            }

        } else if (b != 0xBB) {
            pstate = ST_SYNC1;
        }

        break;


    case ST_HEADER: {
        *pdst++ = b;
        pcount--;

        if (pcount == 0U) {
            uint32_t box_bytes;
            uint32_t body_bytes;

            if (!decode_frame_header(frame_buf[active_idx],
                                     &rx_header,
                                     &box_bytes,
                                     &body_bytes)) {
                LOG_WRN("Invalid frame header; resynchronizing");
                parser_reset_from_error();
                break;
            }

            uint32_t total_needed =
                (uint32_t)sizeof(struct frame_header) + body_bytes;

            /*
             * An oversized but otherwise valid frame can be skipped
             * using its declared body length. Do not write past the
             * application Frame Buffer.
             */
            if (total_needed > FRAME_BUF_SIZE) {
                LOG_WRN("Frame too large: %u bytes; dropping",
                        total_needed);

                parser_release_owned_buffer();

                pcount = body_bytes;
                pstate = ST_DROP_BODY;
                break;
            }

            if (box_bytes > 0U) {
                pcount = box_bytes;
                pstate = ST_BOXES;
            } else {
                pcount = rx_header.image_size;
                pstate = ST_PAYLOAD;
            }
        }

        break;
    }


    case ST_BOXES:
        *pdst++ = b;
        pcount--;

        if (pcount == 0U) {
            pcount = rx_header.image_size;
            pstate = ST_PAYLOAD;
        }

        break;


    case ST_PAYLOAD:
        *pdst++ = b;
        pcount--;

        if (pcount == 0U) {
            uint32_t box_bytes =
                (uint32_t)rx_header.box_count *
                (uint32_t)sizeof(struct bbox_data);

            uint32_t jpeg_offset =
                (uint32_t)sizeof(struct frame_header) + box_bytes;

            uint32_t img_sz = rx_header.image_size;

            uint8_t *jpg = &frame_buf[active_idx][jpeg_offset];

            /*
             * Keep the original main-branch validation:
             * JPEG must begin with FF D8 and end with FF D9
             * exactly at image_size.
             */
            if (jpg[0] != 0xFF ||
                jpg[1] != 0xD8 ||
                jpg[img_sz - 2U] != 0xFF ||
                jpg[img_sz - 1U] != 0xD9) {

                LOG_WRN(
                    "JPEG marker check failed: size=%u "
                    "head=%02X %02X tail=%02X %02X",
                    img_sz,
                    jpg[0], jpg[1],
                    jpg[img_sz - 2U],
                    jpg[img_sz - 1U]);

                parser_reset_from_error();
                break;
            }

            int completed_idx = active_idx;

            struct frame_msg m = {
                .idx = completed_idx,
                .len = jpeg_offset + img_sz,
            };

            /*
             * Transfer ownership to the message queue / TCP sender.
             * If enqueue fails, return the buffer immediately.
             */
            frame_owned = false;

            if (k_msgq_put(&frame_ready_msgq,
                           &m,
                           K_NO_WAIT) != 0) {
                LOG_WRN("Frame queue full; dropping frame");
                k_sem_give(&buf_free_sem[completed_idx]);
            }

            active_idx = (completed_idx + 1) % NUM_FRAME_BUFS;
            parser_to_sync();
        }

        break;


    case ST_DROP_HEADER: {
        if (drop_header_pos < sizeof(drop_header)) {
            drop_header[drop_header_pos++] = b;
        }

        if (drop_header_pos >= sizeof(drop_header)) {
            uint32_t box_bytes;
            uint32_t body_bytes;

            if (!decode_frame_header(drop_header,
                                     &rx_header,
                                     &box_bytes,
                                     &body_bytes)) {
                LOG_WRN("Invalid dropped-frame header");
                parser_to_sync();
                break;
            }

            pcount = body_bytes;

            if (pcount > 0U) {
                pstate = ST_DROP_BODY;
            } else {
                parser_to_sync();
            }
        }

        break;
    }


    case ST_DROP_BODY:
        if (pcount > 0U) {
            pcount--;
        }

        if (pcount == 0U) {
            parser_to_sync();
        }

        break;


    default:
        parser_reset_from_error();
        break;
    }
}


/* ============================================================
 * 11. Parser Thread
 * ============================================================ */

static void parser_thread(void *arg1, void *arg2, void *arg3)
{
    ARG_UNUSED(arg1);
    ARG_UNUSED(arg2);
    ARG_UNUSED(arg3);

    uint8_t chunk[256];

    while (1) {
        k_sem_take(&rb_sem, K_FOREVER);

        while (1) {

            /*
             * If the producer reports byte loss, stop consuming the
             * current stream, release any partially built frame and
             * discard the queued data before resuming.
             */
            if (atomic_get(&rx_stream_discontinuity) != 0) {
                parser_handle_discontinuity();
                continue;
            }

            uint32_t len =
                ring_buf_get(&uart_rb, chunk, sizeof(chunk));

            if (len == 0U) {
                break;
            }

            for (uint32_t i = 0; i < len; i++) {

                /*
                 * An overflow may happen while this local chunk is
                 * being processed. If so, abandon its remaining bytes.
                 */
                if (atomic_get(&rx_stream_discontinuity) != 0) {
                    break;
                }

                parser_feed(chunk[i]);
            }
        }
    }
}


K_THREAD_DEFINE(parser_tid,
                PARSER_STACK_SIZE,
                parser_thread,
                NULL, NULL, NULL,
                PARSER_PRIORITY,
                0,
                0);

/* UART RX restart work item：必須在 Callback 前宣告 */

static void uart_rx_restart_work_handler(struct k_work *work);

K_WORK_DEFINE(rx_restart_work, uart_rx_restart_work_handler);
/* ============================================================
 * 12. UART Async DMA callback
 * ============================================================ */

static void uart_async_cb(const struct device *dev,
                          struct uart_event *evt,
                          void *user_data)
{
    ARG_UNUSED(user_data);

    switch (evt->type) {

    case UART_RX_RDY: {
        const uint8_t *src =
            &evt->data.rx.buf[evt->data.rx.offset];

        uint32_t len = evt->data.rx.len;

        if (len == 0U) {
            break;
        }

        /*
         * When a discontinuity is pending, do not append bytes until
         * the parser thread has flushed the Ring Buffer and reset.
         */
        if (atomic_get(&rx_stream_discontinuity) != 0) {
            break;
        }

        /*
         * Ring Buffer has one producer (this callback) and one
         * consumer (parser_thread). Check the actual number of bytes
         * written; ring_buf_put() may write less than requested.
         */
        uint32_t written = ring_buf_put(&uart_rb, src, len);

        if (written != len) {
            LOG_ERR("Ring Buffer overflow: received=%u stored=%u",
                    len, written);

            request_stream_resync();
        } else {
            k_sem_give(&rb_sem);
        }

        break;
    }


    case UART_RX_BUF_REQUEST: {
        /*
         * The UART driver requests a replacement buffer.
         * Only offer a buffer that has not already been handed to
         * the driver or is currently in use.
         */
        int idx = dma_buffer_reserve();

        if (idx < 0) {
            LOG_ERR("No free DMA RX buffer");

            request_stream_resync();
            break;
        }

        int rc = uart_rx_buf_rsp(dev,
                                 dma_rx_buf[idx],
                                 DMA_BUF_SIZE);

        if (rc != 0) {
            LOG_ERR("uart_rx_buf_rsp failed: %d", rc);

            dma_buffer_release_index(idx);
            request_stream_resync();
        }

        break;
    }


    case UART_RX_BUF_RELEASED:
        /*
         * This is the point at which the driver confirms that the
         * buffer may safely be reused.
         */
        dma_buffer_release_ptr(evt->data.rx_buf.buf);
        break;


    case UART_RX_STOPPED:
        LOG_ERR("UART RX stopped; awaiting RX_DISABLED");

        /*
         * Do not modify parser state here. The parser thread owns it.
         */
        request_stream_resync();
        break;


    case UART_RX_DISABLED:
        /*
         * Restart in thread context rather than directly calling
         * uart_rx_enable() from the async callback.
         */
        LOG_WRN("UART RX disabled; scheduling restart");
        (void)k_work_submit(&rx_restart_work);
        break;


    default:
        break;
    }
}


/* ============================================================
 * 13. UART RX restart work item
 * ============================================================ */



static void uart_rx_restart_work_handler(struct k_work *work)
{
    ARG_UNUSED(work);

    if (uart_dev == NULL || !device_is_ready(uart_dev)) {
        LOG_ERR("Cannot restart UART RX: device not ready");
        return;
    }

    int rc = uart_rx_start_with_free_buffer(uart_dev);

    if (rc != 0) {
        LOG_ERR("Failed to restart UART RX: %d", rc);
        return;
    }

    LOG_INF("UART RX restarted");
}


/* ============================================================
 * 14. Wi-Fi event handlers
 * ============================================================ */

static void wifi_event_handler(
    struct net_mgmt_event_callback *cb,
    uint64_t mgmt_event,
    struct net_if *net_iface)
{
    ARG_UNUSED(cb);
    ARG_UNUSED(net_iface);

    if (mgmt_event == NET_EVENT_WIFI_DISCONNECT_RESULT) {
        l3_ready = false;
    }
}


static void ipv4_event_handler(
    struct net_mgmt_event_callback *cb,
    uint64_t mgmt_event,
    struct net_if *net_iface)
{
    ARG_UNUSED(cb);
    ARG_UNUSED(net_iface);

    if (mgmt_event == NET_EVENT_IPV4_ADDR_ADD) {
        l3_ready = true;
    }
}


/* ============================================================
 * 15. Network setup
 * ============================================================ */

static int setup_network(void)
{
    iface = net_if_get_default();

    if (iface == NULL) {
        LOG_ERR("No default network interface");
        return -ENODEV;
    }

    net_mgmt_init_event_callback(
        &wifi_cb,
        wifi_event_handler,
        NET_EVENT_WIFI_CONNECT_RESULT |
        NET_EVENT_WIFI_DISCONNECT_RESULT);

    net_mgmt_add_event_callback(&wifi_cb);

    net_mgmt_init_event_callback(
        &ipv4_cb,
        ipv4_event_handler,
        NET_EVENT_IPV4_ADDR_ADD);

    net_mgmt_add_event_callback(&ipv4_cb);


    struct wifi_connect_req_params wifi_params = {
        .ssid = MY_WIFI_SSID,
        .ssid_length = strlen(MY_WIFI_SSID),

        .psk = MY_WIFI_PASSWORD,
        .psk_length = strlen(MY_WIFI_PASSWORD),

        .security = WIFI_SECURITY_TYPE_PSK,
        .channel = WIFI_CHANNEL_ANY,
    };

    int rc = net_mgmt(NET_REQUEST_WIFI_CONNECT,
                      iface,
                      &wifi_params,
                      sizeof(wifi_params));

    if (rc != 0) {
        LOG_ERR("Wi-Fi connect request failed: %d", rc);
        return rc;
    }

    LOG_INF("Waiting for Wi-Fi IPv4 address...");

    while (!l3_ready) {
        k_sleep(K_MSEC(500));
    }


    struct zsock_addrinfo *info = NULL;

    rc = getaddrinfo(SERVER_IP, NULL, NULL, &info);

    if (rc != 0 || info == NULL) {
        LOG_ERR("getaddrinfo failed: %d", rc);
        return -EHOSTUNREACH;
    }

    memcpy(&server_addr_in,
           info->ai_addr,
           sizeof(struct sockaddr_in));

    server_addr_in.sin_port = htons(SERVER_PORT);

    freeaddrinfo(info);

    LOG_INF("Network ready");

    return 0;
}


/* ============================================================
 * 16. Main
 * ============================================================ */

int main(void)
{
    int sock = -1;

    /* ---------- LED ---------- */

    if (!gpio_is_ready_dt(&led)) {
        LOG_WRN("LED GPIO is not ready");
    } else {
        gpio_pin_configure_dt(&led, GPIO_OUTPUT_INACTIVE);
        gpio_pin_set_dt(&led, 0);
    }


    /* ---------- UART device ---------- */

    uart_dev = DEVICE_DT_GET(UART_DEV_NODE);

    if (!device_is_ready(uart_dev)) {
        LOG_ERR("UART4 device not ready");
        return -ENODEV;
    }

    struct uart_config ucfg = {
        .baudrate = UART_BAUDRATE,
        .parity = UART_CFG_PARITY_NONE,
        .stop_bits = UART_CFG_STOP_BITS_1,
        .data_bits = UART_CFG_DATA_BITS_8,
        .flow_ctrl = UART_CFG_FLOW_CTRL_NONE,
    };

    int rc = uart_configure(uart_dev, &ucfg);

    if (rc != 0) {
        LOG_ERR("UART configure failed: %d", rc);
        return rc;
    }


    /* ---------- Frame-buffer semaphores ---------- */

    for (int i = 0; i < NUM_FRAME_BUFS; i++) {
        k_sem_init(&buf_free_sem[i], 1, 1);
    }


    /* ---------- Start UART Async DMA RX ---------- */

    rc = uart_callback_set(uart_dev, uart_async_cb, NULL);

    if (rc != 0) {
        LOG_ERR("uart_callback_set failed: %d", rc);
        return rc;
    }

    rc = uart_rx_start_with_free_buffer(uart_dev);

    if (rc != 0) {
        LOG_ERR("Could not start UART DMA RX: %d", rc);
        return rc;
    }

    LOG_INF("UART4 DMA RX started");
    LOG_INF("Baud rate: %u", UART_BAUDRATE);
    LOG_INF("Ring Buffer: %u bytes", UART_RING_SIZE);
    LOG_INF("Frame Buffer: %u bytes x %u",
            FRAME_BUF_SIZE,
            NUM_FRAME_BUFS);


    /* ---------- Wi-Fi ---------- */

    LOG_INF("Waiting for Wi-Fi connection...");

    rc = setup_network();

    if (rc != 0) {
        LOG_ERR("Network setup failed: %d", rc);
        return rc;
    }


    /* ---------- TCP forwarding ---------- */

    while (1) {

        if (!l3_ready) {
            if (sock >= 0) {
                close(sock);
                sock = -1;
            }

            k_sleep(K_SECONDS(1));
            continue;
        }


        if (sock < 0) {
            sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

            if (sock < 0) {
                LOG_ERR("TCP socket creation failed");
                k_sleep(K_SECONDS(2));
                continue;
            }

            struct zsock_timeval tv = {
                .tv_sec = 2,
                .tv_usec = 0,
            };

            setsockopt(sock,
                       SOL_SOCKET,
                       SO_SNDTIMEO,
                       &tv,
                       sizeof(tv));


            if (connect(sock,
                        (struct sockaddr *)&server_addr_in,
                        sizeof(server_addr_in)) == 0) {

                LOG_INF("PC TCP server connected");

            } else {
                LOG_ERR("TCP connect failed");

                close(sock);
                sock = -1;

                k_sleep(K_SECONDS(2));
                continue;
            }
        }


        struct frame_msg m;

        if (k_msgq_get(&frame_ready_msgq,
                       &m,
                       K_MSEC(2000)) != 0) {
            continue;
        }


        gpio_pin_set_dt(&led, 1);

        uint32_t send_start = k_uptime_get_32();

        uint8_t *fb = frame_buf[m.idx];
        uint32_t total_payload = m.len;

        uint32_t sent_bytes = 0;
        const uint32_t chunk_size = 1460;

        struct frame_header *hdr =
            (struct frame_header *)fb;

        uint32_t jpeg_offset =
            (uint32_t)sizeof(struct frame_header) +
            (uint32_t)hdr->box_count *
            (uint32_t)sizeof(struct bbox_data);

        uint8_t *jpeg = fb + jpeg_offset;


        LOG_INF("Frame received: JPEG=%u bytes, boxes=%u",
                hdr->image_size,
                hdr->box_count);

        LOG_INF("JPEG HEAD: %02X %02X | TAIL: %02X %02X",
                jpeg[0],
                jpeg[1],
                jpeg[hdr->image_size - 2U],
                jpeg[hdr->image_size - 1U]);


        bool send_ok = true;

        while (sent_bytes < total_payload) {
            uint32_t send_len = total_payload - sent_bytes;

            if (send_len > chunk_size) {
                send_len = chunk_size;
            }

            int n = send(sock,
                         fb + sent_bytes,
                         send_len,
                         0);

            if (n <= 0) {
                LOG_ERR("TCP send failed: rc=%d", n);

                close(sock);
                sock = -1;

                send_ok = false;
                break;
            }

            sent_bytes += (uint32_t)n;
        }


        uint32_t send_duration =
            k_uptime_get_32() - send_start;


        if (send_ok) {
            LOG_INF("Frame forwarded: bytes=%u, TCP=%u ms",
                    total_payload,
                    send_duration);
        }


        /*
         * Release the completed frame buffer only after all TCP
         * reads have finished, including the failure path.
         */
        k_sem_give(&buf_free_sem[m.idx]);

        gpio_pin_set_dt(&led, 0);
    }

    return 0;
}