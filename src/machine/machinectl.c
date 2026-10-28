#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

/** Escape a CLI value for the local JSON protocol and reject oversized/control-character input. */
static int quote(const char *s, char *out, size_t cap) {
    size_t n = 0;
    if (cap < 3)
        return -1;
    out[n++] = '"';
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        if (*p < 32)
            return -1;
        if (n + 3 >= cap)
            return -1;
        if (*p == '"' || *p == '\\')
            out[n++] = '\\';
        out[n++] = (char)*p;
    }
    out[n++] = '"';
    out[n] = 0;
    return 0;
}
/** Describe supported physical-input commands and return the command-line error exit status. */
static int usage(void) {
    fprintf(stderr, "Usage: machinectl [--socket PATH] snapshot|stop|resume|flush [--scope "
                    "local|global] [--duration HOURS]\n");
    return 2;
}
/** Send one operator action over the local socket and report rejection/timeout through the exit
 * status. */
int main(int argc, char **argv) {
    const char *path = getenv("SOCKET_PATH");
    if (!path || !*path)
        path = "/tmp/machine.sock";
    const char *scope = "local", *duration = NULL, *cmd = NULL;
    int i = 1;
    if (i + 1 < argc && !strcmp(argv[i], "--socket")) {
        path = argv[i + 1];
        i += 2;
    }
    if (i >= argc)
        return usage();
    if (!strcmp(argv[i], "snapshot") || !strcmp(argv[i], "status"))
        cmd = "SNAPSHOT";
    else if (!strcmp(argv[i], "stop"))
        cmd = "STOP";
    else if (!strcmp(argv[i], "resume"))
        cmd = "RESUME";
    else if (!strcmp(argv[i], "flush"))
        cmd = "FLUSH";
    else
        return usage();
    for (i++; i < argc; i += 2) {
        if (i + 1 == argc)
            return usage();
        if (!strcmp(argv[i], "--scope"))
            scope = argv[i + 1];
        else if (!strcmp(argv[i], "--duration"))
            duration = argv[i + 1];
        else
            return usage();
    }
    if (strcmp(scope, "local") && strcmp(scope, "global"))
        return usage();
    char quoted_scope[64], request[512], duration_field[128] = "";
    if (quote(scope, quoted_scope, sizeof quoted_scope))
        return usage();
    if (duration) {
        char *end;
        double hours = strtod(duration, &end);
        if (*end || !isfinite(hours) || hours <= 0 || strcmp(cmd, "FLUSH"))
            return usage();
        snprintf(duration_field, sizeof duration_field, ",\"duration_h\":%.17g", hours);
    }
    snprintf(request, sizeof request, "{\"cmd\":\"%s\",\"scope\":%s%s}\n", cmd, quoted_scope,
             duration_field);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof addr);
    addr.sun_family = AF_UNIX;
    if (strlen(path) >= sizeof addr.sun_path)
        return usage();
    snprintf(addr.sun_path, sizeof addr.sun_path, "%s", path);
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }
    struct timeval timeout = {3, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout);
    if (connect(fd, (struct sockaddr *)&addr, sizeof addr)) {
        perror("connect machine");
        close(fd);
        return 1;
    }
    size_t sent = 0, len = strlen(request);
    while (sent < len) {
        ssize_t n = write(fd, request + sent, len - sent);
        if (n <= 0) {
            perror("write");
            close(fd);
            return 1;
        }
        sent += (size_t)n;
    }
    char reply[32768];
    size_t used = 0;
    while (used < sizeof reply - 1) {
        ssize_t n = read(fd, reply + used, sizeof reply - used - 1);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("read");
            close(fd);
            return 1;
        }
        if (!n)
            break;
        used += (size_t)n;
        if (memchr(reply, '\n', used))
            break;
    }
    close(fd);
    reply[used] = 0;
    if (!used) {
        fprintf(stderr, "Empty controller reply\n");
        return 1;
    }
    fputs(reply, stdout);
    if (reply[used - 1] != '\n')
        putchar('\n');
    return strstr(reply, "\"error\":") || strstr(reply, "\"event\":\"REJECTED\"") ? 1 : 0;
}
