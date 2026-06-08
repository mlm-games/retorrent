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
    private static final String PENDING_INTENT_FILE = "pending_intent";

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        // Save the launch intent to a file before super.onCreate blocks,
        // so the Rust side can read it on cold start without needing JNI
        byte[] data = intentToBytes(getIntent());
        if (data != null) {
            savePendingIntent(data);
        }
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

    private void savePendingIntent(byte[] data) {
        try {
            java.io.FileOutputStream fos = openFileOutput(PENDING_INTENT_FILE, MODE_PRIVATE);
            fos.write(data);
            fos.close();
            Log.i(TAG, "savePendingIntent: saved " + data.length + " bytes");
        } catch (Exception e) {
            Log.e(TAG, "savePendingIntent failed: " + e.getMessage(), e);
        }
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

    private static native void nativeOnNewIntent(byte[] data);
}
