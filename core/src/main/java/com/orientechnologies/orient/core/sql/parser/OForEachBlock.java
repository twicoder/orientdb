/* Generated By:JJTree: Do not edit this line. OForEachBlock.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.sql.executor.ForEachStep;
import com.orientechnologies.orient.core.sql.executor.GlobalLetExpressionStep;
import com.orientechnologies.orient.core.sql.executor.OForEachExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.executor.OUpdateExecutionPlan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// import com.orientechnologies.orient.core.sql.executor.LetExpressionStep;

public class OForEachBlock extends OStatement {

  static int FOREACH_VARIABLE_PROGR = 0;

  protected OIdentifier loopVariable;
  protected OExpression loopValues;
  protected List<OStatement> statements = new ArrayList<>();

  public OForEachBlock(int id) {
    super(id);
  }

  public OForEachBlock(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public OResultSet execute(
      ODatabase db, Object[] args, OCommandContext parentCtx, boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    ctx.setDatabase(db);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);
    OUpdateExecutionPlan executionPlan;
    if (usePlanCache) {
      executionPlan = createExecutionPlan(ctx, false);
    } else {
      executionPlan = (OUpdateExecutionPlan) createExecutionPlanNoCache(ctx, false);
    }

    executionPlan.executeInternal();
    return new OLocalResultSet(executionPlan);
  }

  @Override
  public OResultSet execute(
      ODatabase db, Map params, OCommandContext parentCtx, boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    ctx.setDatabase(db);
    ctx.setInputParameters(params);

    OUpdateExecutionPlan executionPlan;
    if (usePlanCache) {
      executionPlan = createExecutionPlan(ctx, false);
    } else {
      executionPlan = (OUpdateExecutionPlan) createExecutionPlanNoCache(ctx, false);
    }

    executionPlan.executeInternal();
    return new OLocalResultSet(executionPlan);
  }

  public OUpdateExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    OForEachExecutionPlan plan = new OForEachExecutionPlan(ctx);
    int nextProg = ++FOREACH_VARIABLE_PROGR;
    if (FOREACH_VARIABLE_PROGR < 0) {
      FOREACH_VARIABLE_PROGR = 0;
    }
    OIdentifier varName = new OIdentifier("$__ORIENTDB_FOREACH_VAR_" + nextProg);
    plan.chain(new GlobalLetExpressionStep(varName, loopValues, ctx, enableProfiling));
    plan.chain(
        new ForEachStep(loopVariable, new OExpression(varName), statements, ctx, enableProfiling));
    return plan;
  }

  @Override
  public OStatement copy() {
    OForEachBlock result = new OForEachBlock(-1);
    result.loopVariable = loopVariable.copy();
    result.loopValues = loopValues.copy();
    result.statements = statements.stream().map(x -> x.copy()).collect(Collectors.toList());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OForEachBlock that = (OForEachBlock) o;

    if (loopVariable != null ? !loopVariable.equals(that.loopVariable) : that.loopVariable != null)
      return false;
    if (loopValues != null ? !loopValues.equals(that.loopValues) : that.loopValues != null)
      return false;
    return statements != null ? statements.equals(that.statements) : that.statements == null;
  }

  @Override
  public int hashCode() {
    int result = loopVariable != null ? loopVariable.hashCode() : 0;
    result = 31 * result + (loopValues != null ? loopValues.hashCode() : 0);
    result = 31 * result + (statements != null ? statements.hashCode() : 0);
    return result;
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("FOREACH (");
    loopVariable.toString(params, builder);
    builder.append(" IN ");
    loopValues.toString(params, builder);
    builder.append(") {\n");
    for (OStatement stm : statements) {
      stm.toString(params, builder);
      builder.append("\n");
    }
    builder.append("}");
  }

  public void toGenericStatement(StringBuilder builder) {
    builder.append("FOREACH (");
    loopVariable.toGenericStatement(builder);
    builder.append(" IN ");
    loopValues.toGenericStatement(builder);
    builder.append(") {\n");
    for (OStatement stm : statements) {
      stm.toGenericStatement(builder);
      builder.append("\n");
    }
    builder.append("}");
  }

  public boolean containsReturn() {
    for (OStatement stm : this.statements) {
      if (stm instanceof OReturnStatement) {
        return true;
      }
      if (stm instanceof OForEachBlock && ((OForEachBlock) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof OIfStatement && ((OIfStatement) stm).containsReturn()) {
        return true;
      }
    }
    return false;
  }
}
/* JavaCC - OriginalChecksum=071053b057a38c57f3c90d28399615d0 (do not edit this line) */
