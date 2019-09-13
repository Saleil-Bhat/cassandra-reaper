package io.cassandrareaper.storage.postgresql;

import io.cassandrareaper.core.GenericMetric;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.joda.time.DateTime;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public final class GenericMetricMapper implements ResultSetMapper<GenericMetric> {

  @Override
  public GenericMetric map(int index, ResultSet rs, StatementContext ctx) throws SQLException {
    return GenericMetric.builder()
        .withClusterName(rs.getString("cluster"))
        .withHost(rs.getString("host"))
        .withMetricDomain(rs.getString("metric_domain"))
        .withMetricType(rs.getString("metric_type"))
        .withMetricScope(rs.getString("metric_scope"))
        .withMetricName(rs.getString("metric_name"))
        .withMetricAttribute(rs.getString("metric_attribute"))
        .withValue(rs.getDouble("value"))
        .withTs(new DateTime(rs.getTimestamp("ts")))
        .build();
  }
}