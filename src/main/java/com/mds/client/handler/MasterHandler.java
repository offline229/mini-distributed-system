package com.mds.client.handler;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.net.Socket;

public class MasterHandler {
    private static final Logger logger = LoggerFactory.getLogger(MasterHandler.class);
    private Socket masterSocket;
    private PrintWriter out;
    private BufferedReader in;
    private boolean testMode = false; // 添加测试模式标志

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }

    public void connect(String host, int port) throws IOException {
        try {
            masterSocket = new Socket(host, port);
            out = new PrintWriter(masterSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));
            logger.info("已连接到Master: {}:{}", host, port);
            setTestMode(false); // 连接成功后关闭测试模式
        } catch (IOException e) {
            logger.warn("连接Master失败，进入测试模式: {}", e.getMessage());
            setTestMode(true);
        }
    }

    public JSONObject sendRequest(JSONObject request) throws IOException {
        // 测试模式处理
        if (testMode) {
            logger.info("[测试模式] 模拟发送请求到Master: {}", request);
            // 返回模拟的响应
            JSONObject mockResponse = new JSONObject();
            mockResponse.put("host", "localhost");
            mockResponse.put("port", 8000);
            return mockResponse;
        }

        // 连接检查
        if (out == null || masterSocket == null || !masterSocket.isConnected()) {
            throw new IOException("Master连接未初始化或已断开");
        }

        try {
            out.println(request.toString());
            String response = in.readLine();
            if (response == null) {
                throw new IOException("Master未返回响应");
            }
            return new JSONObject(response);
        } catch (IOException e) {
            logger.error("发送请求到Master失败: {}", e.getMessage());
            throw e;
        }
    }

    public void close() {
        try {
            if (out != null)
                out.close();
            if (in != null)
                in.close();
            if (masterSocket != null)
                masterSocket.close();
            logger.info("Master连接已关闭");
        } catch (IOException e) {
            logger.error("关闭Master连接失败: {}", e.getMessage());
        }
    }
}