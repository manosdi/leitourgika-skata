#define _POSIX_C_SOURCE 200809L
#include "common.h"
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

typedef struct {
    int id;
    SharedMemory *shm;
    Channel *out_ch;
    Channel *in_ch;
    sem_t ack_sem;
    MessageType last_reply_type;
} ProcState;

static void *receiver_worker(void *arg) {
    ProcState *st = (ProcState *)arg;
    int peer_id = (st->id == 1) ? 2 : 1;
    Message msg;

    while (1) {
        channel_recv(st->in_ch, &msg);

        if (msg.type == MSG_TERM) {
            printf("\n[P%d] Λήφθηκε μήνυμα τερματισμού (TERM). Έξοδος...\n", st->id);
            fflush(stdout);
            munmap(st->shm, sizeof(SharedMemory));
            exit(EXIT_SUCCESS);
        } else if (msg.type == MSG_DATA) {
            printf("\n[P%d -> P%d]: %s\n> ", peer_id, st->id, msg.text);
            fflush(stdout);
        } else if (msg.type == MSG_ACK || msg.type == MSG_NACK) {
            /* Τα ACK και NACK δεν εμφανίζονται στον χρήστη· ειδοποιούν το νήμα αποστολής */
            st->last_reply_type = msg.type;
            sem_post(&st->ack_sem);
        }
    }
    return NULL;
}

static void *sender_worker(void *arg) {
    ProcState *st = (ProcState *)arg;
    char line[MAX_TEXT_LEN];

    while (1) {
        printf("> ");
        fflush(stdout);

        if (fgets(line, sizeof(line), stdin) == NULL) {
            /* Σε EOF (Ctrl+D) στέλνουμε TERM */
            strcpy(line, "TERM");
        } else {
            size_t len = strlen(line);
            if (len > 0 && line[len - 1] == '\n') {
                line[len - 1] = '\0';
            } else {
                /* Καθαρισμός τυχόν επιπλέον χαρακτήρων πέραν του MAX_TEXT_LEN */
                int c;
                while ((c = getchar()) != '\n' && c != EOF);
            }
        }

        if (line[0] == '\0') {
            continue;
        }

        Message msg;
        memset(&msg, 0, sizeof(Message));

        if (strcmp(line, "TERM") == 0) {
            msg.type = MSG_TERM;
            channel_send(st->out_ch, &msg);
            printf("[P%d] Αποστολή TERM. Τερματισμός...\n", st->id);
            munmap(st->shm, sizeof(SharedMemory));
            exit(EXIT_SUCCESS);
        }

        /* Stop-and-Wait ARQ έως N συνολικές προσπάθειες */
        msg.type = MSG_DATA;
        strncpy(msg.text, line, MAX_TEXT_LEN - 1);
        msg.text[MAX_TEXT_LEN - 1] = '\0';

        int delivered = 0;
        int max_attempts = st->shm->max_attempts;

        for (int attempt = 1; attempt <= max_attempts; attempt++) {
            channel_send(st->out_ch, &msg);

            /* Αναστολή σε σημαφόρο μέχρι να ληφθεί ACK ή NACK */
            sem_wait_safe(&st->ack_sem);

            if (st->last_reply_type == MSG_ACK) {
                delivered = 1;
                break;
            }
            /* Σε NACK επαναλαμβάνεται ο βρόχος εφόσον attempt < max_attempts */
        }

        if (!delivered) {
            printf("[P%d] Σφάλμα: Το μήνυμα δεν παραδόθηκε (εξαντλήθηκαν οι %d προσπάθειες).\n",
                   st->id, max_attempts);
            fflush(stdout);
        }
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 2 || (strcmp(argv[1], "1") != 0 && strcmp(argv[1], "2") != 0)) {
        fprintf(stderr, "Usage: %s <1|2>\n", argv[0]);
        return EXIT_FAILURE;
    }

    int id = atoi(argv[1]);

    int shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
    if (shm_fd == -1) {
        fprintf(stderr, "Σφάλμα: Δεν βρέθηκαν δομές IPC. Εκτελέστε πρώτα το ./start.\n");
        return EXIT_FAILURE;
    }

    SharedMemory *shm = mmap(NULL, sizeof(SharedMemory),
                             PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    if (shm == MAP_FAILED) {
        perror("mmap");
        return EXIT_FAILURE;
    }

    ProcState st;
    st.id = id;
    st.shm = shm;
    if (id == 1) {
        st.out_ch = &shm->p1_to_enc1;
        st.in_ch  = &shm->enc1_to_p1;
    } else {
        st.out_ch = &shm->p2_to_enc2;
        st.in_ch  = &shm->enc2_to_p2;
    }

    if (sem_init(&st.ack_sem, 0, 0) == -1) {
        perror("sem_init");
        munmap(shm, sizeof(SharedMemory));
        return EXIT_FAILURE;
    }

    printf("=== Διεργασία P%d έτοιμη (Πληκτρολογήστε μήνυμα ή TERM για έξοδο) ===\n", id);

    pthread_t t_recv, t_send;
    pthread_create(&t_recv, NULL, receiver_worker, &st);
    pthread_create(&t_send, NULL, sender_worker, &st);

    pthread_join(t_send, NULL);
    pthread_join(t_recv, NULL);

    sem_destroy(&st.ack_sem);
    munmap(shm, sizeof(SharedMemory));
    return EXIT_SUCCESS;
}