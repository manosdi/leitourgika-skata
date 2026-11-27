#include "common.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void sem_wait_safe(sem_t *sem) {
    while (sem_wait(sem) == -1) {
        if (errno != EINTR) {
            perror("sem_wait");
            exit(EXIT_FAILURE);
        }
    }
}

void channel_init(Channel *ch) {
    memset(ch, 0, sizeof(Channel));
    ch->head = 0;
    ch->tail = 0;
    if (sem_init(&ch->mutex, 1, 1) == -1 ||
        sem_init(&ch->empty, 1, BUFFER_SIZE) == -1 ||
        sem_init(&ch->full,  1, 0) == -1) {
        perror("sem_init");
        exit(EXIT_FAILURE);
    }
}

void channel_destroy(Channel *ch) {
    sem_destroy(&ch->mutex);
    sem_destroy(&ch->empty);
    sem_destroy(&ch->full);
}

void channel_send(Channel *ch, const Message *msg) {
    sem_wait_safe(&ch->empty);
    sem_wait_safe(&ch->mutex);

    ch->buffer[ch->tail] = *msg;
    ch->tail = (ch->tail + 1) % BUFFER_SIZE;

    sem_post(&ch->mutex);
    sem_post(&ch->full);
}

void channel_recv(Channel *ch, Message *msg) {
    sem_wait_safe(&ch->full);
    sem_wait_safe(&ch->mutex);

    *msg = ch->buffer[ch->head];
    ch->head = (ch->head + 1) % BUFFER_SIZE;

    sem_post(&ch->mutex);
    sem_post(&ch->empty);
}

/* 32-bit FNV-1a Hash -> 8 χαρακτήρες hex */
void compute_checksum(const char *text, char out_hex[CHECKSUM_LEN]) {
    uint32_t hash = 2166136261u;
    for (const unsigned char *p = (const unsigned char *)text; *p != '\0'; p++) {
        hash ^= *p;
        hash *= 16777619u;
    }
    snprintf(out_hex, CHECKSUM_LEN, "%08x", hash);
}