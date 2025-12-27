#ifndef JUNO_SCAN_H
#define JUNO_SCAN_H

#ifdef __cplusplus
extern "C" {
#endif

char *juno_scan_scan_tx_json(const char *req_json);
void juno_scan_string_free(char *s);

#ifdef __cplusplus
}
#endif

#endif // JUNO_SCAN_H

