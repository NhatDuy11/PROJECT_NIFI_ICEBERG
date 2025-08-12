FROM apache/nifi:1.26.0

# Copy custom NARs into NiFi's lib directory
COPY nifi-extensions/*.nar /opt/nifi/nifi-current/lib/
