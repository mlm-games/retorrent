package dev.mlm.retorrent;

import android.app.NativeActivity;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.util.Log;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class RetorrentActivity extends NativeActivity {
    private static final String TAG = "Retorrent";
    private static byte[] pendingLaunchIntent;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        // Capture the launch intent before super.onCreate blocks
        pendingLaunchIntent = intentToBytes(getIntent());
        super.onCreate(savedInstanceState);
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        Log.i(TAG, "onNewIntent action=" + intent.getAction() + " data=" + intent.getData());
        if (handleViewIntent(intent)) {
            setIntent(new Intent(Intent.ACTION_MAIN));
            Log.i(TAG, "onNewIntent cleared after processing");
        }
    }

    private boolean handleViewIntent(Intent intent) {
        byte[] data = intentToBytes(intent);
        if (data == null) return false;
        nativeOnNewIntent(data);
        return true;
    }

    private byte[] intentToBytes(Intent intent) {
        if (intent == null) {
            Log.w(TAG, "intentToBytes: null intent");
            return null;
        }
        if (!Intent.ACTION_VIEW.equals(intent.getAction())) {
            Log.w(TAG, "intentToBytes: not VIEW action: " + intent.getAction());
            return null;
        }
        Uri uri = intent.getData();
        if (uri == null) {
            Log.w(TAG, "intentToBytes: no data URI");
            return null;
        }

        String scheme = uri.getScheme();
        Log.i(TAG, "intentToBytes scheme=" + scheme + " uri=" + uri);

        if ("magnet".equals(scheme)) {
            byte[] data = uri.toString().getBytes(StandardCharsets.UTF_8);
            Log.i(TAG, "intentToBytes magnet " + data.length + " bytes");
            return data;
        }

        try {
            InputStream is = getContentResolver().openInputStream(uri);
            if (is == null) {
                Log.w(TAG, "intentToBytes openInputStream returned null");
                return null;
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            while ((n = is.read(buf)) >= 0) {
                baos.write(buf, 0, n);
            }
            is.close();
            byte[] data = baos.toByteArray();
            Log.i(TAG, "intentToBytes read " + data.length + " bytes from " + uri);
            return data;
        } catch (Exception e) {
            Log.e(TAG, "intentToBytes failed: " + e.getMessage(), e);
            return null;
        }
    }

    public static byte[] getAndClearPendingLaunchIntent() {
        byte[] data = pendingLaunchIntent;
        pendingLaunchIntent = null;
        return data;
    }

    private static native void nativeOnNewIntent(byte[] data);
}
