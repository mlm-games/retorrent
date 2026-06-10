package org.mlm.retorrent

import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.IBinder

class TorrentService : Service() {
    override fun onCreate() {
        super.onCreate()
        nativeOnCreate(this)
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == Intent.ACTION_VIEW) {
            intent.data?.let { uri ->
                try {
                    val inputStream = contentResolver.openInputStream(uri) ?: return START_STICKY
                    val bytes = inputStream.use { it.readBytes() }
                    nativeOnTorrentData(bytes)
                } catch (_: Exception) {}
            }
        }
        return START_STICKY
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onDestroy() {
        nativeOnDestroy()
        super.onDestroy()
    }

    companion object {
        init {
            System.loadLibrary("retorrent")
        }

        @JvmStatic private external fun nativeOnCreate(context: Context)
        @JvmStatic private external fun nativeOnDestroy()
        @JvmStatic private external fun nativeOnTorrentData(data: ByteArray)
    }
}
