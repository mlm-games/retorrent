package dev.mlm.retorrent;

import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.os.IBinder;

public class TorrentService extends Service {
    static {
        System.loadLibrary("retorrent");
    }
    public void onCreate() {
        super.onCreate();
        nativeOnCreate(this);
    }
    public int onStartCommand(Intent i, int f, int sid) {
        return START_STICKY;
    }
    public IBinder onBind(Intent i) {
        return null;
    }
    public void onDestroy() {
        nativeOnDestroy();
        super.onDestroy();
    }
    private static native void nativeOnCreate(Context context);
    private static native void nativeOnDestroy();
}
