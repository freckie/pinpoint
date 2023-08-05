/*
 * Copyright 2014 NAVER Corp.
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

package com.navercorp.pinpoint.plugin.jdbc.tibero;

import com.navercorp.pinpoint.bootstrap.context.DatabaseInfo;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.DefaultDatabaseInfo;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.JdbcUrlParserV2;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.StringMaker;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.UnKnownDatabaseInfo;
import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.plugin.jdbc.tibero.parser.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author freckie
 */
public class TiberoJdbcUrlParser implements JdbcUrlParserV2 {

    // jdbc:tibero:thin:@${HOST}:${PORT}:${DB}
    private static final String URL_PREFIX = "jdbc:tibero:";

    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());

    @Override
    public DatabaseInfo parse(String jdbcUrl) {
        if (jdbcUrl == null) {
            logger.info("jdbcUrl must not be null");
            return UnKnownDatabaseInfo.INSTANCE;
        }
        if (!jdbcUrl.startsWith(URL_PREFIX)) {
            logger.info("jdbcUrl has invalid prefix.(url:{}, prefix:{})", jdbcUrl, URL_PREFIX);
            return UnKnownDatabaseInfo.INSTANCE;
        }

        DatabaseInfo result = null;
        try {
            return parse0(jdbcUrl);
        } catch (Exception e) {
            logger.info("TiberoJdbcUrl parse error. url: {}, Caused: {}", jdbcUrl, e.getMessage(), e);
            result = UnKnownDatabaseInfo.createUnknownDataBase(TiberoConstants.TIBERO, TiberoConstants.TIBERO_EXECUTE_QUERY, jdbcUrl);
        }
        return result;
    }

    private DatabaseInfo parse0(String jdbcUrl) {
        StringMaker maker = new StringMaker(jdbcUrl);
        maker.after(URL_PREFIX).after(":");
        String description = maker.after('@').value().trim();
        if (description.startsWith("(")) {
            return parseNetConnectionUrl(jdbcUrl);
        } else {
            return parseSimpleUrl(jdbcUrl, maker);
        }
    }

    private DatabaseInfo parseNetConnectionUrl(String url) {
        TiberoNetConnectionDescriptorParser parser = new TiberoNetConnectionDescriptorParser(url);
        KeyValue<?> keyValue = parser.parse();
        return createTiberoDatabaseInfo(keyValue, url);
    }

    private DefaultDatabaseInfo parseSimpleUrl(String url, StringMaker maker) {
        String host = maker.before(':').value();
        String port = maker.next().after(':').before(':', '/').value();
        String databaseId = maker.next().afterLast(':', '/').value();

        List<String> hostList = new ArrayList<>(1);
        hostList.add(host + ":" + port);
        return new DefaultDatabaseInfo(TiberoConstants.TIBERO, TiberoConstants.TIBERO_EXECUTE_QUERY, url, url, hostList, databaseId);
    }

    private DatabaseInfo createTiberoDatabaseInfo(KeyValue<?> keyValue, String url) {
        final DatabaseSpec databaseSpec = newDatabaseSpec(keyValue);
        if (databaseSpec == null) {
            return UnKnownDatabaseInfo.createUnknownDataBase(TiberoConstants.TIBERO, TiberoConstants.TIBERO_EXECUTE_QUERY, url);
        }

        String databaseId = databaseSpec.getDatabaseId();
        List<String> jdbcHost = databaseSpec.getJdbcHost();
        return new DefaultDatabaseInfo(TiberoConstants.TIBERO, TiberoConstants.TIBERO_EXECUTE_QUERY, url, url, jdbcHost, databaseId);
    }

    private DatabaseSpec newDatabaseSpec(KeyValue<?> keyValue) {
        if (ParserUtils.compare(Description.DESCRIPTION, keyValue)) {
            return new Description(keyValue);
        } else if (ParserUtils.compare(DescriptionList.DESCRIPTION_LIST, keyValue)) {
            return new DescriptionList(keyValue);
        }
        return null;
    }

    @Override
    public ServiceType getServiceType() {
        return TiberoConstants.TIBERO;
    }

}
