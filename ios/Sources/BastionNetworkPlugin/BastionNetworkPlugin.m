#import <Foundation/Foundation.h>
#import <Capacitor/Capacitor.h>

// THIS IS WHAT MAKES CAPACITOR RECOGNIZE YOUR PLUGIN
CAP_PLUGIN(BastionNetworkPlugin, "BastionNetworkPlugin",
    CAP_PLUGIN_METHOD(sendBroadcastUDP, CAPPluginReturnPromise);
    CAP_PLUGIN_METHOD(connectTCP, CAPPluginReturnPromise);
    CAP_PLUGIN_METHOD(sendTCP, CAPPluginReturnPromise);
    CAP_PLUGIN_METHOD(disconnectTCP, CAPPluginReturnPromise);
)
