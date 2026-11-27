#ifndef COMMON_H
#define COMMON_H

#include <semaphore.h>
#include <stddef.h>
#include <stdint.h>

#define SHM_NAME "/k22_os_msg_ipc"
#define MAX_TEXT_LEN 512
#define CHECKSUM_LEN 9      /* 8 hex χαρακτήρες + '\0' */
#define BUFFER_SIZE 8       /* Μέγεθος κυκλικού buffer ανά ακμή */

typedef enum {
    MSG_DATA = 1,
    MSG_ACK  = 2,
    MSG_NACK = 3,
    MSG_TERM = 4
} MessageType;

typedef struct {
    MessageType type;
    char text[MAX_TEXT_LEN];
    char checksum[CHECKSUM_LEN];
} Message;

typedef struct {
    Message buffer[BUFFER_SIZE];
    int head;
    int tail;
    sem_t mutex;
    sem_t empty;
    sem_t full;
} Channel;

typedef struct {
    double p;               /* Πιθανότητα αλλοίωσης χαρακτήρα [0, 1] */
    int max_attempts;       /* Μέγιστος αριθμός προσπαθειών N >= 1 */

    /* 4 αμφίδρομες ακμές = 8 μονόδρομα κανάλια */
    Channel p1_to_enc1;
    Channel enc1_to_p1;
    Channel enc1_to_chan;
    Channel chan_to_enc1;
    Channel chan_to_enc2;
    Channel enc2_to_chan;
    Channel enc2_to_p2;
    Channel p2_to_enc2;
} SharedMemory;

void sem_wait_safe(sem_t *sem);
void channel_init(Channel *ch);
void channel_destroy(Channel *ch);
void channel_send(Channel *ch, const Message *msg);
void channel_recv(Channel *ch, Message *msg);
void compute_checksum(const char *text, char out_hex[CHECKSUM_LEN]);

#endif