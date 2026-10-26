/*
 * A minimal Stintmate IPC client in C: connects to an endpoint that offers the method
 * "t.echo" and checks that it returns its params.
 *
 *   echo.exe <pipe path> <key file>
 *
 * Build (MSVC): cl /I crates\stintmate-ipc-ffi\include examples\c\echo.c stintmate_ipc.dll.lib
 * Payloads are MessagePack: here the fixstr "hello" is written by hand (0xa5 + 5 bytes).
 */
#include <stdio.h>
#include <string.h>

#include "stintmate_ipc.h"

static void print_last_error(const char *what) {
    char message[512];
    stintmate_ipc_last_error_message(message, sizeof message);
    fprintf(stderr, "%s failed: %s\n", what, message);
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: echo <pipe path> <key file>\n");
        return 2;
    }
    printf("stintmate-ipc %s\n", stintmate_ipc_version());

    stintmate_ipc_peer_info info = {0};
    info.app = "c-example";
    info.name = "C example";
    info.version = "1.0";

    stintmate_ipc_key_source key = {0};
    key.struct_size = sizeof key;
    key.path = argv[2];

    stintmate_ipc_client *client = NULL;
    if (stintmate_ipc_client_new(argv[1], &info, &key, NULL, &client) != STINTMATE_IPC_OK) {
        print_last_error("client_new");
        return 1;
    }
    stintmate_ipc_client_start(client);
    if (stintmate_ipc_client_wait_connected(client, 5000, NULL) != STINTMATE_IPC_OK) {
        print_last_error("wait_connected");
        stintmate_ipc_client_free(client);
        return 1;
    }

    const uint8_t params[] = {0xa5, 'h', 'e', 'l', 'l', 'o'};
    stintmate_ipc_buffer result = {0};
    int32_t status = stintmate_ipc_client_call(client, "t.echo", params, sizeof params, NULL, &result);
    int ok = status == STINTMATE_IPC_OK && result.len == sizeof params &&
             memcmp(result.ptr, params, sizeof params) == 0;
    if (status != STINTMATE_IPC_OK) {
        print_last_error("call");
    }
    stintmate_ipc_free_buffer(result);
    stintmate_ipc_client_free(client);
    if (!ok) {
        return 1;
    }
    printf("echo ok\n");
    return 0;
}
