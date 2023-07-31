## Tibero JDBC Driver
* Since: Pinpoint 2.5.2
* See: Tibero6

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
profiler.jdbc.tibero.tracesqlbindvalue=false

# Disallow profiling of {createStatement, prepareStatement, prepareCall}.
profiler.jdbc.tibero.disallow3methods=true
~~~
