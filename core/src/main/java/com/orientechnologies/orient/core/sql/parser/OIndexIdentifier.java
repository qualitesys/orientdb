/* Generated By:JJTree: Do not edit this line. OIndexIdentifier.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;

import java.util.Map;

public class OIndexIdentifier extends SimpleNode {

  public enum Type {
    INDEX, VALUES, VALUESASC, VALUESDESC
  }

  protected Type       type;
  protected String     indexNameString;
  protected OIndexName indexName;

  public OIndexIdentifier(int id) {
    super(id);
  }

  public OIndexIdentifier(String indexName, Type type){
    this.indexNameString = indexName;
    this.type = type;
  }

  public OIndexIdentifier(OrientSql p, int id) {
    super(p, id);
  }

  /**
   * Accept the visitor.
   **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    switch (type) {
    case INDEX:
      builder.append("INDEX");
      break;
    case VALUES:
      builder.append("INDEXVALUES");
      break;
    case VALUESASC:
      builder.append("INDEXVALUESASC");
      break;
    case VALUESDESC:
      builder.append("INDEXVALUESDESC");
      break;
    }
    builder.append(":");
    if (indexNameString != null) {
      builder.append(indexNameString);
    } else {
      indexName.toString(params, builder);
    }
  }

  public String getIndexName() {
    if (indexName != null) {
      return indexName.toString();
    }
    return indexNameString;
  }

  public Type getType() {
    return type;
  }

  public OIndexIdentifier copy() {
    OIndexIdentifier result = new OIndexIdentifier(-1);
    result.type = type;
    result.indexNameString = indexNameString;
    result.indexName = indexName.copy();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OIndexIdentifier that = (OIndexIdentifier) o;

    if (type != that.type)
      return false;
    if (indexNameString != null ? !indexNameString.equals(that.indexNameString) : that.indexNameString != null)
      return false;
    if (indexName != null ? !indexName.equals(that.indexName) : that.indexName != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = type != null ? type.hashCode() : 0;
    result = 31 * result + (indexNameString != null ? indexNameString.hashCode() : 0);
    result = 31 * result + (indexName != null ? indexName.hashCode() : 0);
    return result;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", type.toString());
    result.setProperty("indexNameString", indexNameString);

    if (indexName != null) {
      result.setProperty("indexName", indexName.serialize());
    }
    return result;
  }

  public void deserialize(OResult fromResult) {
    type = Type.valueOf(fromResult.getProperty("type"));
    indexNameString = fromResult.getProperty("indexNameString");

    if (fromResult.getProperty("indexName") != null) {
      indexName = new OIndexName(-1);
      indexName.deserialize(fromResult.getProperty("indexName"));
    }
  }

  public void setType(Type type) {
    this.type = type;
  }

  public void setIndexNameString(String indexNameString) {
    this.indexNameString = indexNameString;
  }

  public void setIndexName(OIndexName indexName) {
    this.indexName = indexName;
  }
}
/* JavaCC - OriginalChecksum=025f134fd4b27b84210738cdb6dd027c (do not edit this line) */

