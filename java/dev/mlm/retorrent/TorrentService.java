package dev.mlm.retorrent;

import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.os.IBinder;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

public class TorrentService extends Service {
    static {
        System.loadLibrary("retorrent");
    }
    public void onCreate() {
        super.onCreate();
        nativeOnCreate(this);
    }
    public int onStartCommand(Intent i, int f, int sid) {
        if (i != null && Intent.ACTION_VIEW.equals(i.getAction())) {
            Uri uri = i.getData();
            if (uri != null) {
                try {
                    InputStream is = getContentResolver().openInputStream(uri);
                    if (is != null) {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        byte[] buf = new byte[4096];
                        int n;
                        while ((n = is.read(buf)) >= 0) {
                            baos.write(buf, 0, n);
                        }
                        is.close();
                        nativeOnTorrentData(baos.toByteArray());
                    }
                } catch (Exception e) {
                    // ignore
                }
            }
        }
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
    private static native void nativeOnTorrentData(byte[] data);
}
