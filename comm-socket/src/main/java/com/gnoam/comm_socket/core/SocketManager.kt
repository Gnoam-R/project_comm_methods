package com.gnoam.comm_socket.core

import android.util.Log
import com.gnoam.comm_socket.data.NetworkPacket
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Socket

class SocketManager(
    private val ip: String,
    private val port: Int
) {
    @Volatile
    private var socket: Socket? = null

    // TCP 소켓 연결
    suspend fun connect(): Boolean = withContext(Dispatchers.IO) {
        if (socket?.isConnected == true) return@withContext true
        try {
            Log.d(TAG, "onTcpConnect ip: $ip port: $port")
            socket = Socket().also { it.connect(InetSocketAddress(ip, port), CONNECT_TIMEOUT_MS) }
            true
        } catch (e: IOException) {
            Log.e(TAG, "connect error: $e")
            false
        }
    }

    // 서버가 연결을 끊거나 disConnect() 호출 시 정상 완료, 그 외 IO 에러는 collector로 전달
    fun receiveMessage(): Flow<NetworkPacket> = flow {
        val s = socket ?: throw IllegalStateException("not connected")
        val input = s.getInputStream()
        val buffer = ByteArray(BUFFER_SIZE)
        try {
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                emit(NetworkPacket(String(buffer, 0, read)))
            }
        } catch (e: IOException) {
            if (!s.isClosed) throw e
        }
    }.flowOn(Dispatchers.IO)

    suspend fun sendMessage(msg: String): Boolean = withContext(Dispatchers.IO) {
        val s = socket ?: return@withContext false
        try {
            s.getOutputStream().apply {
                write(msg.toByteArray())
                flush()
            }
            true
        } catch (e: IOException) {
            Log.e(TAG, "sendMessage error: $e")
            false
        }
    }

    // blocking read()는 코루틴 취소로 안 풀린다 — 소켓 close가 read를 깨운다
    suspend fun disConnect() = withContext(Dispatchers.IO) {
        socket?.close()
        socket = null
    }

    companion object {
        private val crlf = "\r\n".toByteArray()
        private val TAG = SocketManager::class.simpleName ?: "UnknownTag"
        private const val CONNECT_TIMEOUT_MS = 5_000
        private const val BUFFER_SIZE = 8192
    }
}
