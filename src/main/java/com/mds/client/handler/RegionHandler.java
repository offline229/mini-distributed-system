package com.mds.client.handler;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.Socket;

public class RegionHandler {
    private static final Logger logger = LoggerFactory.getLogger(RegionHandler.class);
    private Socket regionSocket;
    private PrintWriter out;
    private BufferedReader in;

    public void connect(String host, int port) throws IOException {
        regionSocket = new Socket(host, port);
        out = new PrintWriter(regionSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(regionSocket.getInputStream()));
        logger.info("已连接到Region: {}:{}", host, port);
    }

    public Object sendRequest(JSONObject request) throws IOException {
        if (out == null || regionSocket == null || !regionSocket.isConnected()) {
            throw new IOException("Region连接未初始化或已断开");
        }

        try {
            out.println(request.toString());
            String response = in.readLine();
            return new JSONObject(response).get("data");
        } catch (IOException e) {
            logger.error("发送请求到Region失败: {}", e.getMessage());
            throw e;
        }
    }

    public void close() {
        try {
            if (out != null)
                out.close();
            if (in != null)
                in.close();
            if (regionSocket != null)
                regionSocket.close();
            logger.info("Region连接已关闭");
        } catch (IOException e) {
            logger.error("关闭Region连接失败: {}", e.getMessage());
        }
    }
}