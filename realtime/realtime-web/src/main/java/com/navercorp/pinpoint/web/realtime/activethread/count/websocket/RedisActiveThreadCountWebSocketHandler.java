/*
 * Copyright 2023 NAVER Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.navercorp.pinpoint.web.realtime.activethread.count.websocket;

import com.google.gson.Gson;
import com.navercorp.pinpoint.web.realtime.activethread.count.dto.ActiveThreadCountResponse;
import com.navercorp.pinpoint.web.realtime.activethread.count.service.ActiveThreadCountService;
import com.navercorp.pinpoint.web.realtime.activethread.count.service.ActiveThreadCountSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.lang.NonNull;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import reactor.core.Disposable;

import java.io.IOException;
import java.util.Objects;

/**
 * @author youngjin.kim2
 */
public class RedisActiveThreadCountWebSocketHandler {

    private static final Logger logger = LogManager.getLogger(RedisActiveThreadCountWebSocketHandler.class);
    private static final Gson gson = new Gson();

    private final ActiveThreadCountService atcService;

    public RedisActiveThreadCountWebSocketHandler(ActiveThreadCountService atcSessionFactory) {
        this.atcService = Objects.requireNonNull(atcSessionFactory, "atcSessionFactory");
    }

    public void afterConnectionEstablished(@NonNull WebSocketSession session) {
        logger.info("ATC Connection Established. session: {}", session);
        HandlerSession.initialize(session, this.atcService);
    }

    public void afterConnectionClosed(@NonNull WebSocketSession session, @NonNull CloseStatus status) {
        logger.info("ATC Connection Closed. session: {}, status: {}", session, status);
        HandlerSession.dispose(session);
    }

    public void handleActiveThreadCount(WebSocketSession wsSession, String applicationName) {
        logger.info("ATC Requested. session: {}, applicationName: {}", wsSession, applicationName);
        HandlerSession handlerSession = HandlerSession.get(wsSession);
        if (handlerSession == null) {
            logger.error("CustomSession is not initialized");
            return;
        }
        handlerSession.start(applicationName);
    }

    private static class HandlerSession implements Disposable {

        private static final String ATTR_KEY = "handlerSession";

        private final WebSocketSession wsSession;
        private final ActiveThreadCountService atcService;
        private String applicationName;
        private ActiveThreadCountSession atcSession;

        private final Object lock = new Object();

        private HandlerSession(WebSocketSession wsSession, ActiveThreadCountService atcService) {
            this.wsSession = wsSession;
            this.atcService = atcService;
        }

        public static HandlerSession get(WebSocketSession wsSession) {
            Object t = wsSession.getAttributes().get(ATTR_KEY);
            if (t instanceof HandlerSession) {
                return (HandlerSession) t;
            }
            return null;
        }

        public static void initialize(WebSocketSession wsSession, ActiveThreadCountService atcService) {
            HandlerSession prev = get(wsSession);
            if (prev != null) {
                return;
            }
            HandlerSession handlerSession = new HandlerSession(wsSession, atcService);
            wsSession.getAttributes().put(ATTR_KEY, handlerSession);
        }

        public static void dispose(WebSocketSession wsSession) {
            HandlerSession that = get(wsSession);
            if (that != null) {
                that.dispose();
            }
        }

        void start(String applicationName) {
            synchronized (lock) {
                if (this.applicationName != null) {
                    logger.error("Already started with application {}", this.applicationName);
                    return;
                }
                start0(applicationName);
            }
        }

        private void start0(String applicationName) {
            try {
                this.applicationName = applicationName;
                this.atcSession = buildATCSession(applicationName);
            } catch (Exception e) {
                logger.error("Failed to start atc session");
                throw new RuntimeException(e);
            }
        }

        private ActiveThreadCountSession buildATCSession(String applicationName) throws Exception {
            ActiveThreadCountSession atcSession = this.atcService.getSession(applicationName);
            atcSession.start().subscribe(this::sendMessage);
            return atcSession;
        }

        @Override
        public void dispose() {
            synchronized (lock) {
                if (this.atcSession != null) {
                    this.atcSession.dispose();
                }
                this.applicationName = null;
                this.atcSession = null;
            }
        }

        private void sendMessage(ActiveThreadCountResponse response) {
            try {
                TextMessage message = new TextMessage(gson.toJson(response));
                synchronized (lock) {
                    this.wsSession.sendMessage(message);
                }
            } catch (IOException e) {
                logger.error("Failed to send message to {}", this.wsSession);
            }
        }

    }

}
