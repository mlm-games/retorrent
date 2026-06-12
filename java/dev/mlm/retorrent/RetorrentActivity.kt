package org.mlm.retorrent

import android.app.NativeActivity
import android.content.Intent
import android.os.Build
import android.os.Bundle
import android.util.Log
import android.view.WindowInsets
import java.nio.charset.StandardCharsets

class RetorrentActivity : NativeActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        intentToBytes(intent)?.let { savePendingIntent(it) }
        super.onCreate(savedInstanceState)

        if (Build.VERSION.SDK_INT >= 30) {
            window.setDecorFitsSystemWindows(false)
            window.decorView.setOnApplyWindowInsetsListener { view, insets ->
                val systemBars = insets.getInsets(WindowInsets.Type.systemBars())
                val ime = insets.getInsets(WindowInsets.Type.ime())
                nativeOnWindowInsets(
                    systemBars.top.toFloat(),
                    systemBars.bottom.toFloat(),
                    systemBars.left.toFloat(),
                    systemBars.right.toFloat(),
                    ime.bottom.toFloat(),
                )
                view.onApplyWindowInsets(insets)
            }
        } else {
            @Suppress("DEPRECATION")
            window.decorView.setOnApplyWindowInsetsListener { view, insets ->
                nativeOnWindowInsets(
                    insets.systemWindowInsetTop.toFloat(),
                    insets.systemWindowInsetBottom.toFloat(),
                    insets.systemWindowInsetLeft.toFloat(),
                    insets.systemWindowInsetRight.toFloat(),
                    insets.systemWindowInsetBottom.toFloat(),
                )
                view.onApplyWindowInsets(insets)
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        Log.i(TAG, "onNewIntent action=${intent.action} data=${intent.data}")
        if (handleViewIntent(intent)) {
            setIntent(Intent(Intent.ACTION_MAIN))
            Log.i(TAG, "onNewIntent cleared after processing")
        }
    }

    private fun handleViewIntent(intent: Intent): Boolean {
        val data = intentToBytes(intent) ?: return false
        nativeOnNewIntent(data)
        return true
    }

    private fun savePendingIntent(data: ByteArray) {
        try {
            val tmp = java.io.File(filesDir, "$PENDING_INTENT_FILE.tmp")
            val dst = java.io.File(filesDir, PENDING_INTENT_FILE)

            tmp.writeBytes(data)

            if (!tmp.renameTo(dst)) {
                throw java.io.IOException("rename failed")
            }

            Log.i(TAG, "savePendingIntent: saved ${data.size} bytes")
        } catch (e: Exception) {
            Log.e(TAG, "savePendingIntent failed", e)
        }
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

    private fun intentToBytes(intent: Intent?): ByteArray? {
        if (intent == null) { Log.w(TAG, "intentToBytes: null intent"); return null }
        if (intent.action != Intent.ACTION_VIEW) { return null }
        val uri = intent.data ?: run { Log.w(TAG, "intentToBytes: no data URI"); return null }
        Log.i(TAG, "intentToBytes scheme=${uri.scheme} uri=$uri")
        if (uri.scheme == "magnet") return uri.toString().toByteArray(StandardCharsets.UTF_8)
        return try {
            readTorrentBytes(uri)
        } catch (e: Exception) {
            Log.e(TAG, "intentToBytes failed: ${e.message}", e)
            null
        }
    }

    companion object {
        private const val TAG = "Retorrent"
        private const val PENDING_INTENT_FILE = "pending_intent"

        @JvmStatic private external fun nativeOnNewIntent(data: ByteArray)
        @JvmStatic private external fun nativeOnWindowInsets(
            topPx: Float,
            bottomPx: Float,
            leftPx: Float,
            rightPx: Float,
            imeBottomPx: Float,
        )
    }
}
