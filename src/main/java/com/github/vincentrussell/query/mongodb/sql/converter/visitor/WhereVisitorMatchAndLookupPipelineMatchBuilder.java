package com.github.vincentrussell.query.mongodb.sql.converter.visitor;

import org.apache.commons.lang.mutable.MutableBoolean;

import com.github.vincentrussell.query.mongodb.sql.converter.holder.ExpressionHolder;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.schema.Column;

//Generate lookup match from where. For optimization, this must combine with "on" part of joined collection
//TODO: For optimization with multiple joins this visitor could return some map with table asociated where expression 
public class WhereVisitorMatchAndLookupPipelineMatchBuilder extends ExpressionVisitorAdapter{
	private String baseAliasTable;
	private ExpressionHolder outputMatch = null;//This expression will have the where part of baseAliasTable
	private MutableBoolean haveOrExpression = new MutableBoolean();//This flag will be true is there is some "or" expression. It that case match expression go in the main pipeline after lookup. TODO: Better optimization to get the baseTablePart even with "ors" in where 
	private boolean isBaseAliasOrValue;
	
	public WhereVisitorMatchAndLookupPipelineMatchBuilder(String baseAliasTable, ExpressionHolder outputMatch, MutableBoolean haveOrExpression) {
		this.baseAliasTable = baseAliasTable;
		this.outputMatch = outputMatch;
		this.haveOrExpression = haveOrExpression;
	}
	
	private ExpressionHolder setOrAndExpression(ExpressionHolder baseExp, Expression newExp) {
		Expression exp;
		if(baseExp.getExpression() != null) {
			exp = new AndExpression(baseExp.getExpression(), newExp);
		}
		else {
			exp = newExp;
		}
		baseExp.setExpression(exp);
		return baseExp;
	}
	
	@Override
    public void visit(Column column) {
		if(SqlUtils.isColumn(column)){
			this.isBaseAliasOrValue = SqlUtils.isTableAliasOfColumn(column, this.baseAliasTable);
		}
    }
	
	@Override
    public void visit(OrExpression expr) {
		this.haveOrExpression.setValue(true);
    }
	
	@Override
    public void visit(IsNullExpression expr) {
        if(this.isBaseAliasOrValue) {
			this.setOrAndExpression(outputMatch,expr);
		}
    }
	
	//Default  with expresion copy
    protected void visitBinaryExpression(BinaryExpression expr) {
    	this.isBaseAliasOrValue = true;
    	expr.getLeftExpression().accept(this);
    	if(!this.isBaseAliasOrValue) {
    		expr.getRightExpression().accept(this);
		}
    	else {
    		expr.getRightExpression().accept(this);
            if(this.isBaseAliasOrValue && !(expr instanceof AndExpression || expr instanceof OrExpression)) {
    			this.setOrAndExpression(outputMatch,expr);
    		}
    	}
    }
    	
}
