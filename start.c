#define _POSIX_C_SOURCE 200809L
#include "common.h"
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    Channel *from_proc;
    Channel *to_proc;
    Channel *from_chan;
    Channel *to_chan;
    SharedMemory *shm;
} EncArgs;

typedef struct {
    Channel *in_ch;
    Channel *out_ch;
    double p;
    unsigned int seed;
    SharedMemory *shm;
} ChanDirArgs;

static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s -p <probability [0, 1]> -n <attempts (>= 1)>\n", prog);
}

/* Επιλέγει τυχαίο εκτυπώσιμο ASCII χαρακτήρα (32..126) αυστηρά διαφορετικό από τον original */
static char random_different_char(char original, unsigned int *seed) {
    int offset = rand_r(seed) % 94;
    char candidate = (char)(32 + offset);
    if (candidate >= original) {
        candidate++;
    }
    return candidate;
}

static void corrupt_string(char *str, double p, unsigned int *seed) {
    if (p <= 0.0) return;
    for (size_t i = 0; str[i] != '\0'; i++) {
        double r = (double)rand_r(seed) / ((double)RAND_MAX + 1.0);
        if (r < p) {
            str[i] = random_different_char(str[i], seed);
        }
    }
}

/* --- Νήματα διεργασιών ENC1 & ENC2 --- */
static void *enc_outgoing_worker(void *arg) {
    EncArgs *args = (EncArgs *)arg;
    Message msg;

    while (1) {
        channel_recv(args->from_proc, &msg);
        if (msg.type == MSG_TERM) {
            channel_send(args->to_chan, &msg);
            munmap(args->shm, sizeof(SharedMemory));
            exit(EXIT_SUCCESS);
        }
        if (msg.type == MSG_DATA) {
            compute_checksum(msg.text, msg.checksum);
            channel_send(args->to_chan, &msg);
        }
    }
    return NULL;
}

static void *enc_incoming_worker(void *arg) {
    EncArgs *args = (EncArgs *)arg;
    Message msg;

    while (1) {
        channel_recv(args->from_chan, &msg);
        if (msg.type == MSG_TERM) {
            channel_send(args->to_proc, &msg);
            munmap(args->shm, sizeof(SharedMemory));
            exit(EXIT_SUCCESS);
        }
        if (msg.type == MSG_ACK || msg.type == MSG_NACK) {
            channel_send(args->to_proc, &msg);
        } else if (msg.type == MSG_DATA) {
            char expected[CHECKSUM_LEN];
            compute_checksum(msg.text, expected);

            Message reply;
            memset(&reply, 0, sizeof(Message));

            if (strncmp(msg.checksum, expected, CHECKSUM_LEN) == 0) {
                /* Έγκυρο checksum: αφαίρεση checksum, παράδοση στην P και αποστολή ACK */
                memset(msg.checksum, 0, CHECKSUM_LEN);
                channel_send(args->to_proc, &msg);

                reply.type = MSG_ACK;
                channel_send(args->to_chan, &reply);
            } else {
                /* Εσφαλμένο checksum: απόρριψη και αποστολή NACK */
                reply.type = MSG_NACK;
                channel_send(args->to_chan, &reply);
            }
        }
    }
    return NULL;
}

static void run_enc(SharedMemory *shm, int id) {
    EncArgs args;
    args.shm = shm;
    if (id == 1) {
        args.from_proc = &shm->p1_to_enc1;
        args.to_proc   = &shm->enc1_to_p1;
        args.from_chan = &shm->chan_to_enc1;
        args.to_chan   = &shm->enc1_to_chan;
    } else {
        args.from_proc = &shm->p2_to_enc2;
        args.to_proc   = &shm->enc2_to_p2;
        args.from_chan = &shm->chan_to_enc2;
        args.to_chan   = &shm->enc2_to_chan;
    }

    pthread_t t_out, t_in;
    pthread_create(&t_out, NULL, enc_outgoing_worker, &args);
    pthread_create(&t_in,  NULL, enc_incoming_worker, &args);
    pthread_join(t_out, NULL);
    pthread_join(t_in, NULL);
}

/* --- Νήματα διεργασίας CHAN --- */
static void *chan_dir_worker(void *arg) {
    ChanDirArgs *args = (ChanDirArgs *)arg;
    Message msg;

    while (1) {
        channel_recv(args->in_ch, &msg);
        if (msg.type == MSG_TERM) {
            channel_send(args->out_ch, &msg);
            munmap(args->shm, sizeof(SharedMemory));
            exit(EXIT_SUCCESS);
        }
        if (msg.type == MSG_DATA) {
            corrupt_string(msg.text, args->p, &args->seed);
            corrupt_string(msg.checksum, args->p, &args->seed);
        }
        /* Τα ACK και NACK περνούν αναλλοίωτα */
        channel_send(args->out_ch, &msg);
    }
    return NULL;
}

static void run_chan(SharedMemory *shm) {
    unsigned int base_seed = (unsigned int)(time(NULL) ^ getpid());
    ChanDirArgs dir12 = {
        .in_ch = &shm->enc1_to_chan,
        .out_ch = &shm->chan_to_enc2,
        .p = shm->p,
        .seed = base_seed + 1,
        .shm = shm
    };
    ChanDirArgs dir21 = {
        .in_ch = &shm->enc2_to_chan,
        .out_ch = &shm->chan_to_enc1,
        .p = shm->p,
        .seed = base_seed + 2,
        .shm = shm
    };

    pthread_t t12, t21;
    pthread_create(&t12, NULL, chan_dir_worker, &dir12);
    pthread_create(&t21, NULL, chan_dir_worker, &dir21);
    pthread_join(t12, NULL);
    pthread_join(t21, NULL);
}

int main(int argc, char *argv[]) {
    int opt;
    int has_p = 0, has_n = 0;
    double p = 0.0;
    long n = 0;

    while ((opt = getopt(argc, argv, "p:n:")) != -1) {
        char *endptr = NULL;
        switch (opt) {
            case 'p':
                errno = 0;
                p = strtod(optarg, &endptr);
                if (errno != 0 || *optarg == '\0' || *endptr != '\0' ||
                    isnan(p) || isinf(p) || p < 0.0 || p > 1.0) {
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
                }
                has_p = 1;
                break;
            case 'n':
                errno = 0;
                n = strtol(optarg, &endptr, 10); //δεκαδικο
                if (errno != 0 || *optarg == '\0' || *endptr != '\0' || n < 1) {
                    print_usage(argv[0]);
                    return EXIT_FAILURE;
                }
                has_n = 1;
                break;
            default:
                print_usage(argv[0]);
                return EXIT_FAILURE;
        }
    }

    if (!has_p || !has_n || optind != argc) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    /* Καθαρισμός τυχόν παλαιού υπολείμματος πριν τη δημιουργία */
    shm_unlink(SHM_NAME);

    int shm_fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("shm_open");
        return EXIT_FAILURE;
    }

    if (ftruncate(shm_fd, sizeof(SharedMemory)) == -1) {
        perror("ftruncate");
        shm_unlink(SHM_NAME);
        return EXIT_FAILURE;    
    }

    SharedMemory *shm = mmap(NULL, sizeof(SharedMemory),
        PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    close(shm_fd);
    if (shm == MAP_FAILED) {
        perror("mmap");
        shm_unlink(SHM_NAME);
        return EXIT_FAILURE;
    }

    shm->p = p;
    shm->max_attempts = (int)n;

    channel_init(&shm->p1_to_enc1);
    channel_init(&shm->enc1_to_p1);
    channel_init(&shm->enc1_to_chan);
    channel_init(&shm->chan_to_enc1);
    channel_init(&shm->chan_to_enc2);
    channel_init(&shm->enc2_to_chan);
    channel_init(&shm->enc2_to_p2);
    channel_init(&shm->p2_to_enc2);

    pid_t pids[3];

    if ((pids[0] = fork()) == 0) {
        run_enc(shm, 1);
        return EXIT_SUCCESS;
    }
    if ((pids[1] = fork()) == 0) {
        run_enc(shm, 2);
        return EXIT_SUCCESS;
    }
    if ((pids[2] = fork()) == 0) {
        run_chan(shm);
        return EXIT_SUCCESS;
    }

    printf("[START] Σύστημα ενεργό (p = %.3f, N = %d).\n", p, (int)n);
    printf("[START] Εκκινήστε σε ξεχωριστά τερματικά: ./proc 1 και ./proc 2\n");

    /* Αναμονή τερματισμού και των 3 διεργασιών (ENC1, ENC2, CHAN) */
    for (int i = 0; i < 3; i++) {
        waitpid(pids[i], NULL, 0);
    }

    /* Αποδέσμευση σημαφόρων και διαμοιραζόμενης μνήμης */
    channel_destroy(&shm->p1_to_enc1);
    channel_destroy(&shm->enc1_to_p1);
    channel_destroy(&shm->enc1_to_chan);
    channel_destroy(&shm->chan_to_enc1);
    channel_destroy(&shm->chan_to_enc2);
    channel_destroy(&shm->enc2_to_chan);
    channel_destroy(&shm->enc2_to_p2);
    channel_destroy(&shm->p2_to_enc2);

    munmap(shm, sizeof(SharedMemory));
    shm_unlink(SHM_NAME);

    printf("[START] Όλες οι διεργασίες τερμάτισαν και οι δομές IPC αποδεσμεύτηκαν.\n");
    return EXIT_SUCCESS;
}