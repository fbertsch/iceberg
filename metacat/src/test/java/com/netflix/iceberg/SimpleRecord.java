package com.netflix.iceberg;

import org.apache.iceberg.relocated.com.google.common.base.Objects;

public class SimpleRecord
{
  private Integer id;
  private String data;

  public SimpleRecord() {}

  public SimpleRecord(Integer id, String data) {
    this.id = id;
    this.data = data;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleRecord record = (SimpleRecord) o;
    return Objects.equal(id, record.id) && Objects.equal(data, record.data);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, data);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{\"id\"=");
    buffer.append(id);
    buffer.append(",\"data\"=\"");
    buffer.append(data);
    buffer.append("\"}");
    return buffer.toString();
  }
}