package org.mlm.retorrent

import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.IBinder
import android.util.Log

class TorrentService : Service() {
    override fun onCreate() {
        super.onCreate()
        nativeOnCreate(this)
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == Intent.ACTION_VIEW) {
            intent.data?.let { uri ->
                try {
                    val bytes = readTorrentBytes(uri) ?: return START_NOT_STICKY
                    nativeOnTorrentData(bytes)
                } catch (e: Exception) {
                    Log.e(TAG, "onStartCommand: failed to read torrent data", e)
                }
            }
        }
        return START_NOT_STICKY
    }

    override fun onTimeout(startId: Int, fgsType: Int) {
        Log.w(TAG, "onTimeout: foreground service timeout (startId=$startId, fgsType=$fgsType)")
        try {
            nativeOnForegroundServiceTimeout()
        } catch (e: Throwable) {
            Log.e(TAG, "onTimeout: native call failed", e)
        }
        stopSelf(startId)
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onDestroy() {
        nativeOnDestroy()
        super.onDestroy()
    }

    private fun readTorrentBytes(uri: android.net.Uri): ByteArray? {
        val maxBytes = 32 * 1024 * 1024
        return contentResolver.openInputStream(uri)?.use { input ->
            val out = java.io.ByteArrayOutputStream()
            val buf = ByteArray(16 * 1024)
            var total = 0

            while (true) {
                val n = input.read(buf)
                if (n < 0) break
                total += n
                if (total > maxBytes) {
                    throw IllegalArgumentException(
                        "Torrent file too large (max ${maxBytes / (1024 * 1024)} MiB)"
                    )
                }
                out.write(buf, 0, n)
            }

            out.toByteArray()
        }
    }

    companion object {
        private const val TAG = "TorrentService"

        init {
            System.loadLibrary("retorrent")
        }

        @JvmStatic private external fun nativeOnCreate(context: Context)
        @JvmStatic private external fun nativeOnDestroy()
        @JvmStatic private external fun nativeOnTorrentData(data: ByteArray)
        @JvmStatic private external fun nativeOnForegroundServiceTimeout()
    }
}
