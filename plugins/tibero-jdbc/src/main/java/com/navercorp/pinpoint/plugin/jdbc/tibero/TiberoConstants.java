/*
 * Copyright 2014 NAVER Corp.
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

import com.navercorp.pinpoint.common.trace.ServiceType;
import com.navercorp.pinpoint.common.trace.ServiceTypeProvider;

/**
 * @author freckie
 *
 */
public final class TiberoConstants {
    private TiberoConstants() {
    }

    public static final String TIBERO_SCOPE = "TIBERO_SCOPE";

    public static final ServiceType TIBERO = ServiceTypeProvider.getByCode(2420);
    public static final ServiceType TIBERO_EXECUTE_QUERY = ServiceTypeProvider.getByCode(2421);
}