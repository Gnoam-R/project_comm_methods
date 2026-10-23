package com.gnoam.comm_socket.data

data class NetworkPacket (
    val rawData: String,
    val timestamp: Long = System.currentTimeMillis()
)