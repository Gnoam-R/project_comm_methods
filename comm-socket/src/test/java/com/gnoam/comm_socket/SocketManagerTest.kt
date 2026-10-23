package com.gnoam.comm_socket

import com.gnoam.comm_socket.core.SocketManager
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.ServerSocket
import kotlin.concurrent.thread

class SocketManagerTest {
    @Test
    fun sendAndReceive_echo() = runBlocking {
        val server = ServerSocket(0)
        thread {
            server.accept().use { c ->
                val buf = ByteArray(64)
                val n = c.getInputStream().read(buf)
                c.getOutputStream().write(buf, 0, n)
            }
        }

        val manager = SocketManager("127.0.0.1", server.localPort)
        assertTrue(manager.connect())
        assertTrue(manager.sendMessage("sn01_on_000"))
        assertEquals("sn01_on_000", manager.receiveMessage().first().rawData)

        // 서버가 끊은 뒤에는 flow가 예외 없이 완료돼야 한다
        assertTrue(manager.receiveMessage().toList().isEmpty())
        manager.disConnect()
        server.close()
    }
}
