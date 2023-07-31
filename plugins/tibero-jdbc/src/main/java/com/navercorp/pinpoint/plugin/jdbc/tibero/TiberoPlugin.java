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

import com.navercorp.pinpoint.bootstrap.instrument.InstrumentClass;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentException;
import com.navercorp.pinpoint.bootstrap.instrument.InstrumentMethod;
import com.navercorp.pinpoint.bootstrap.instrument.Instrumentor;
import com.navercorp.pinpoint.bootstrap.instrument.MethodFilter;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformCallback;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplate;
import com.navercorp.pinpoint.bootstrap.instrument.transformer.TransformTemplateAware;
import com.navercorp.pinpoint.bootstrap.interceptor.Interceptor;
import com.navercorp.pinpoint.bootstrap.interceptor.scope.ExecutionPolicy;
import com.navercorp.pinpoint.bootstrap.logging.PLogger;
import com.navercorp.pinpoint.bootstrap.logging.PLoggerFactory;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPlugin;
import com.navercorp.pinpoint.bootstrap.plugin.ProfilerPluginSetupContext;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.BindValueAccessor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.DatabaseInfoAccessor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.JdbcUrlParserV2;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.ParsingResultAccessor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.PreparedStatementBindingMethodFilter;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.CallableStatementRegisterOutParameterInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.ConnectionCloseInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.DriverConnectInterceptorV2;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.PreparedStatementBindVariableInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.PreparedStatementCreateInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.PreparedStatementExecuteQueryInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.StatementCreateInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.StatementExecuteQueryInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.StatementExecuteUpdateInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.TransactionCommitInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.TransactionRollbackInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.jdbc.interceptor.TransactionSetAutoCommitInterceptor;
import com.navercorp.pinpoint.bootstrap.plugin.util.InstrumentUtils;

import java.security.ProtectionDomain;
import java.util.List;

import static com.navercorp.pinpoint.common.util.VarArgs.va;

/**
 * @author freckie
 */
public class TiberoPlugin implements ProfilerPlugin, TransformTemplateAware {

    private static final String TIBERO_SCOPE = TiberoConstants.TIBERO_SCOPE;

    private final PLogger logger = PLoggerFactory.getLogger(this.getClass());

    private static final String CLASS_STATEMENT_WRAPPER = "com.tmax.tibero.jdbc.driver.TbStatement";
    private static final String CLASS_STATEMENT = "com.tmax.tibero.jdbc.TbStatement";
    private static final String CLASS_PREPARED_STATEMENT_WRAPPER = "com.tmax.tibero.jdbc.driver.TbPreparedStatementImpl";
    private static final String CLASS_PREPARED_STATEMENT = "com.tmax.tibero.jdbc.driver.TbPreparedStatement";
    private static final String CLASS_CALLABLE_STATEMENT_WRAPPER = "com.tmax.tibero.jdbc.driver.TbCallableStatementImpl";
    private static final String CLASS_CALLABLE_STATEMENT = "com.tmax.tibero.jdbc.driver.TbCallableStatement";

    private TransformTemplate transformTemplate;

    private final JdbcUrlParserV2 jdbcUrlParser = new TiberoJdbcUrlParser();

    @Override
    public void setup(ProfilerPluginSetupContext context) {
        TiberoConfig config = new TiberoConfig(context.getConfig());
        if (!config.isPluginEnable()) {
            logger.info("{} disabled", this.getClass().getSimpleName());
            return;
        }
        logger.info("{} config:{}", this.getClass().getSimpleName(), config);

        context.addJdbcUrlParser(jdbcUrlParser);

        addConnectionTransformer();
        addDriverTransformer();
        addPreparedStatementTransformer();
        addCallableStatementTransformer();
        addStatementTransformer();
    }

    private void addConnectionTransformer() {
        transformTemplate.transform("com.tmax.tibero.jdbc.driver.TbConnection", PhysicalConnectionTransform.class);
    }

    public static class PhysicalConnectionTransform implements TransformCallback {

        @Override
        public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
            InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);
            target.addField(DatabaseInfoAccessor.class);

            TiberoConfig config = new TiberoConfig(instrumentor.getProfilerConfig());

            if (!config.isProfileDisallow3Methods()) {
                // close
                InstrumentUtils.findMethod(target, "close")
                        .addScopedInterceptor(ConnectionCloseInterceptor.class, TIBERO_SCOPE);

                // createStatement
                final Class<? extends Interceptor> statementCreate = StatementCreateInterceptor.class;
                InstrumentUtils.findMethod(target, "createStatement")
                        .addScopedInterceptor(statementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "createStatement", "int", "int")
                        .addScopedInterceptor(statementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "createStatement", "int", "int", "int")
                        .addScopedInterceptor(statementCreate, TIBERO_SCOPE);

                // prepareStatement
                final Class<? extends Interceptor> preparedStatementCreate = PreparedStatementCreateInterceptor.class;
                InstrumentUtils.findMethod(target, "prepareStatement", "java.lang.String")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "prepareStatement", "java.lang.String", "boolean")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "prepareStatement", "java.lang.String", "int")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "prepareStatement", "java.lang.String", "int", "int")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "prepareStatement", "java.lang.String", "int", "int", "boolean")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "prepareStatement", "java.lang.String", "int", "int", "int")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "prepareStatement", "java.lang.String", "int[]")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "prepareStatement", "java.lang.String", "java.lang.String[]")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);

                // prepareCall
                InstrumentUtils.findMethod(target, "prepareCall", "java.lang.String")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "prepareCall", "java.lang.String", "int", "int")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);
                InstrumentUtils.findMethod(target, "prepareCall", "java.lang.String", "int", "int", "int")
                        .addScopedInterceptor(preparedStatementCreate, TIBERO_SCOPE);
            }

            if (config.isProfileSetAutoCommit()) {
                InstrumentUtils.findMethod(target, "setAutoCommit",  "boolean")
                        .addScopedInterceptor(TransactionSetAutoCommitInterceptor.class, TIBERO_SCOPE);
            }

            if (config.isProfileCommit()) {
                InstrumentUtils.findMethod(target, "commit")
                        .addScopedInterceptor(TransactionCommitInterceptor.class, TIBERO_SCOPE);
            }

            if (config.isProfileRollback()) {
                InstrumentUtils.findMethod(target, "rollback")
                        .addScopedInterceptor(TransactionRollbackInterceptor.class, TIBERO_SCOPE);
            }

            return target.toBytecode();
        }
    }

    private void addDriverTransformer() {
        transformTemplate.transform("com.tmax.tibero.jdbc.TbDriver", TiberoDriverTransformer.class);
    }

    public static class TiberoDriverTransformer implements TransformCallback {
        @Override
        public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
            InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

            InstrumentUtils.findMethod(target, "connect",  "com.tmax.tibero.jdbc.data.ConnectionInfo")
                    .addScopedInterceptor(DriverConnectInterceptorV2.class, va(TiberoConstants.TIBERO), TIBERO_SCOPE, ExecutionPolicy.ALWAYS);

            InstrumentUtils.findMethod(target, "connect",  "java.lang.String", "java.util.Properties")
                    .addScopedInterceptor(DriverConnectInterceptorV2.class, va(TiberoConstants.TIBERO), TIBERO_SCOPE, ExecutionPolicy.ALWAYS);

            return target.toBytecode();
        }
    }

    private void addPreparedStatementTransformer() {
        transformTemplate.transform(CLASS_PREPARED_STATEMENT, PreparedStatementTransformer.class);
        transformTemplate.transform(CLASS_PREPARED_STATEMENT_WRAPPER, PreparedStatementTransformer.class);
    }

    public static class PreparedStatementTransformer implements TransformCallback {

        @Override
        public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
            if (className.equals(CLASS_PREPARED_STATEMENT)) {
                if (instrumentor.exist(loader, CLASS_PREPARED_STATEMENT_WRAPPER, protectionDomain)) {
                    return null;
                }
            }
            final TiberoConfig config = new TiberoConfig(instrumentor.getProfilerConfig());

            InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

            target.addField(DatabaseInfoAccessor.class);
            target.addField(ParsingResultAccessor.class);
            target.addField(BindValueAccessor.class);

            int maxBindValueSize = config.getMaxSqlBindValueSize();

            final Class<? extends Interceptor> preparedStatementInterceptor = PreparedStatementExecuteQueryInterceptor.class;
            InstrumentUtils.findMethod(target, "execute", "java.lang.String")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "execute", "java.lang.String", "int")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "execute", "java.lang.String", "int[]")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "execute", "java.lang.String", "java.lang.String[]")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "executeQuery")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "executeQuery", "java.lang.String")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "executeUpdate")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "executeUpdate", "java.lang.String")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "executeUpdate", "java.lang.String", "int")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "executeUpdate", "java.lang.String", "int[]")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "executeUpdate", "java.lang.String", "java.lang.String[]")
                    .addScopedInterceptor(preparedStatementInterceptor, va(maxBindValueSize), TIBERO_SCOPE);

            if (config.isTraceSqlBindValue()) {
                MethodFilter filter = new PreparedStatementBindingMethodFilter();
                List<InstrumentMethod> declaredMethods = target.getDeclaredMethods(filter);
                for (InstrumentMethod method : declaredMethods) {
                    method.addScopedInterceptor(PreparedStatementBindVariableInterceptor.class, TIBERO_SCOPE);
                }
            }

            return target.toBytecode();
        }
    }

    private void addCallableStatementTransformer() {
        transformTemplate.transform(CLASS_CALLABLE_STATEMENT, CallableStatementTransformer.class);
        transformTemplate.transform(CLASS_CALLABLE_STATEMENT_WRAPPER, CallableStatementTransformer.class);
    }

    public static class CallableStatementTransformer implements TransformCallback {

        @Override
        public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
            InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);
            if (className.equals(CLASS_CALLABLE_STATEMENT)) {
                if (instrumentor.exist(loader, CLASS_CALLABLE_STATEMENT_WRAPPER, protectionDomain)) {
                    return null;
                }
            }

            target.addField(DatabaseInfoAccessor.class);
            target.addField(ParsingResultAccessor.class);
            target.addField(BindValueAccessor.class);

            final Class<? extends Interceptor> callableStatementInterceptor = CallableStatementRegisterOutParameterInterceptor.class;
            InstrumentUtils.findMethod(target, "registerOutParameter", "int", "int")
                    .addScopedInterceptor(callableStatementInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "registerOutParameter", "int", "int", "int")
                    .addScopedInterceptor(callableStatementInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "registerOutParameter", "int", "int", "java.lang.String")
                    .addScopedInterceptor(callableStatementInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "registerOutParameter", "java.lang.String", "int")
                    .addScopedInterceptor(callableStatementInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "registerOutParameter", "java.lang.String", "int", "int")
                    .addScopedInterceptor(callableStatementInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "registerOutParameter", "java.lang.String", "int", "java.lang.String")
                    .addScopedInterceptor(callableStatementInterceptor, TIBERO_SCOPE);

            return target.toBytecode();
        }
    }

    private void addStatementTransformer() {
        transformTemplate.transform(CLASS_STATEMENT_WRAPPER, StatementTransformer.class);
    }

    public static class StatementTransformer implements TransformCallback {

        @Override
        public byte[] doInTransform(Instrumentor instrumentor, ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain, byte[] classfileBuffer) throws InstrumentException {
            InstrumentClass target = instrumentor.getInstrumentClass(loader, className, classfileBuffer);

            target.addField(DatabaseInfoAccessor.class);

            final Class<? extends Interceptor> executeQueryInterceptor = StatementExecuteQueryInterceptor.class;
            InstrumentUtils.findMethod(target, "executeQuery", "java.lang.String")
                    .addScopedInterceptor(executeQueryInterceptor, TIBERO_SCOPE);

            final Class<? extends Interceptor> executeUpdateInterceptor = StatementExecuteUpdateInterceptor.class;
            InstrumentUtils.findMethod(target, "executeUpdate", "java.lang.String")
                    .addScopedInterceptor(executeUpdateInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "executeUpdate",  "java.lang.String", "int")
                    .addScopedInterceptor(executeUpdateInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "executeUpdate",  "java.lang.String", "int[]")
                    .addScopedInterceptor(executeUpdateInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "executeUpdate",  "java.lang.String", "java.lang.String[]")
                    .addScopedInterceptor(executeUpdateInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "execute",  "java.lang.String")
                    .addScopedInterceptor(executeUpdateInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "execute",  "java.lang.String", "int")
                    .addScopedInterceptor(executeUpdateInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "execute",  "java.lang.String", "int[]")
                    .addScopedInterceptor(executeUpdateInterceptor, TIBERO_SCOPE);
            InstrumentUtils.findMethod(target, "execute",  "java.lang.String", "java.lang.String[]")
                    .addScopedInterceptor(executeUpdateInterceptor, TIBERO_SCOPE);

            return target.toBytecode();
        }
    }

    @Override
    public void setTransformTemplate(TransformTemplate transformTemplate) {
        this.transformTemplate = transformTemplate;
    }
}
