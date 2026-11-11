#include <jni.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

JNIEXPORT jint JNICALL
Java_ru_bast_plugins_network_BastionNetworkPlugin_configureKeepAlive(
    JNIEnv *env, jclass type, jint fd, jint idle, jint interval, jint count) {
    (void)env;
    (void)type;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle)) < 0 ||
        setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval)) < 0 ||
        setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(count)) < 0) return -1;
    return 0;
}
