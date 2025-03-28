/* Generated By:JJTree: Do not edit this line. OBeginStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.tx.OTransaction;
import java.util.Map;

public class OBeginStatement extends OSimpleExecStatement {
  protected OIdentifier isolation;

  public OBeginStatement(int id) {
    super(id);
  }

  public OBeginStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public OResultSet executeSimple(OCommandContext ctx) {
    ctx.getDatabase().begin();
    OInternalResultSet result = new OInternalResultSet();
    OResultInternal item = new OResultInternal();
    item.setProperty("operation", "begin");
    if (isolation != null) {
      ctx.getDatabase()
          .getTransaction()
          .setIsolationLevel(OTransaction.ISOLATION_LEVEL.valueOf(isolation.getStringValue()));
      item.setProperty("isolation", isolation.getStringValue());
    }
    result.add(item);
    return result;
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("BEGIN");
    if (isolation != null) {
      builder.append(" ISOLATION ");
      isolation.toString(params, builder);
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("BEGIN");
    if (isolation != null) {
      builder.append(" ISOLATION ");
      isolation.toGenericStatement(builder);
    }
  }

  @Override
  public OBeginStatement copy() {
    OBeginStatement result = new OBeginStatement(-1);
    result.isolation = isolation == null ? null : isolation.copy();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OBeginStatement that = (OBeginStatement) o;

    if (isolation != null ? !isolation.equals(that.isolation) : that.isolation != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return isolation != null ? isolation.hashCode() : 0;
  }
}
/* JavaCC - OriginalChecksum=aaa994acbe63cc4169fe33144d412fed (do not edit this line) */
