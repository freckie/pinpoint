## Tibero JDBC Driver
* Since: Pinpoint 2.6.0
* See: http://tmaxtibero.com

### Pinpoint Configuration
pinpoint.config

#### JDBC options.
~~~
# Profile Tibero DB.
profiler.jdbc.tibero=true
# Allow profiling of setautocommit.
profiler.jdbc.tibero.setautocommit=false
# Allow profiling of commit.
profiler.jdbc.tibero.commit=false
# Allow profiling of rollback.
profiler.jdbc.tibero.rollback=false
# Trace bindvalues for Tibero PreparedStatements (overrides profiler.jdbc.tracesqlbindvalue)
#profiler.jdbc.tibero.tracesqlbindvalue=true
~~~