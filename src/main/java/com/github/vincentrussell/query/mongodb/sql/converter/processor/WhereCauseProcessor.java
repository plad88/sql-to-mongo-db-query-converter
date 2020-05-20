package com.github.vincentrussell.query.mongodb.sql.converter.processor;

import com.github.vincentrussell.query.mongodb.sql.converter.FieldType;

import com.github.vincentrussell.query.mongodb.sql.converter.ParseException;
import com.github.vincentrussell.query.mongodb.sql.converter.util.DateFunction;
import com.github.vincentrussell.query.mongodb.sql.converter.util.ObjectIdFunction;
import com.github.vincentrussell.query.mongodb.sql.converter.util.RegexFunction;
import com.github.vincentrussell.query.mongodb.sql.converter.util.SqlUtils;
import com.google.common.collect.Lists;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import org.bson.BsonArray;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WhereCauseProcessor {

    private final FieldType defaultFieldType;
    private final Map<String, FieldType> fieldNameToFieldTypeMapping;

    public WhereCauseProcessor(FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping) {
        this.defaultFieldType = defaultFieldType;
        this.fieldNameToFieldTypeMapping = fieldNameToFieldTypeMapping;
    }
    
    //Parse comparative expression != = < > => <= into mongo expr
    private void parseComparativeExpr(Document query, Expression leftExpression, Expression rightExpression, String comparatorType)  throws ParseException{
    	String operator = "$" + comparatorType;
    	if (Function.class.isInstance(leftExpression)) {
        	Document doc = new Document();
        	Object leftParse = parseExpression(new Document(), leftExpression, rightExpression);
        	Object rightParse = parseExpression(new Document(), rightExpression, leftExpression); 
        	doc.put(operator, Arrays.asList(leftParse, (SqlUtils.isColumn(rightExpression)&&!rightExpression.toString().startsWith("$")?"$" + rightParse:rightParse)));
        	query.put("$expr", doc);
        } else if (SqlUtils.isColumn(leftExpression) && SqlUtils.isColumn(rightExpression)){
        	Document doc = new Document();
        	String leftName = ((Column)leftExpression).getName(false);
        	String rightName = ((Column)rightExpression).getName(false);
        	doc.put(operator,Arrays.asList((leftName.startsWith("$")?leftName:"$" + leftName), (rightName.startsWith("$")?rightName:"$" + rightName)));
        	query.put("$expr", doc);
        }
        else if (Function.class.isInstance(rightExpression)){
        	Document doc = new Document();
        	Object leftParse = parseExpression(new Document(), rightExpression, leftExpression);
        	Object rightParse = parseExpression(new Document(), leftExpression, rightExpression);
        	doc.put(operator, Arrays.asList(leftParse, (SqlUtils.isColumn(leftExpression)&&!leftExpression.toString().startsWith("$")?"$" + rightParse:rightParse)));
        	query.put("$expr", doc); 
    	} else if (SqlUtils.isColumn(leftExpression)){
        	Document subdocument = new Document();
        	if(operator.equals("$eq")) {
                query.put(parseExpression(new Document(), leftExpression, rightExpression).toString(), 
                		parseExpression(new Document(), rightExpression, leftExpression));
        	}
        	else {
        		subdocument.put(operator,parseExpression(new Document(), rightExpression, leftExpression));
                query.put(parseExpression(new Document(), leftExpression, rightExpression).toString(), 
                		subdocument);
        	}
        } else {
        	Document doc = new Document();
        	Object leftParse = parseExpression(new Document(), leftExpression, rightExpression);
        	doc.put(operator, Arrays.asList(leftParse, SqlUtils.nonFunctionToNode(rightExpression)));
        	query.put("$expr", doc);
        }
    }

    public Object parseExpression(Document query, Expression incomingExpression, Expression otherSide) throws ParseException {
        if (ComparisonOperator.class.isInstance(incomingExpression)) {
            RegexFunction regexFunction = SqlUtils.isRegexFunction(incomingExpression);
            DateFunction dateFunction = SqlUtils.isDateFunction(incomingExpression);
            ObjectIdFunction objectIdFunction = SqlUtils.isObjectIdFunction(this, incomingExpression);
            if (regexFunction != null) {
                Document regexDocument = new Document("$regex", regexFunction.getRegex());
                if (regexFunction.getOptions() != null) {
                    regexDocument.append("$options", regexFunction.getOptions());
                }
                query.put(regexFunction.getColumn(), regexDocument);
            } else if (dateFunction!=null) {
                query.put(dateFunction.getColumn(),
                    new Document(dateFunction.getComparisonExpression(), dateFunction.getDate()));
            } else if (objectIdFunction != null) {
                query.put(objectIdFunction.getColumn(), objectIdFunction.toDocument());
            } else if (EqualsTo.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((EqualsTo) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((EqualsTo) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "eq");
            } else if (NotEqualsTo.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((NotEqualsTo) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((NotEqualsTo) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "ne");
            } else if (GreaterThan.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((GreaterThan) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((GreaterThan) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "gt");
            } else if (MinorThan.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((MinorThan) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((MinorThan) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "lt");
            } else if (GreaterThanEquals.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((GreaterThanEquals) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((GreaterThanEquals) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "gte");
            } else if (MinorThanEquals.class.isInstance(incomingExpression)) {
                final Expression leftExpression = ((MinorThanEquals) incomingExpression).getLeftExpression();
                final Expression rightExpression = ((MinorThanEquals) incomingExpression).getRightExpression();
                parseComparativeExpr(query, leftExpression, rightExpression, "lte");
            }
        } else if(LikeExpression.class.isInstance(incomingExpression)
                && Column.class.isInstance(((LikeExpression)incomingExpression).getLeftExpression())
                && (StringValue.class.isInstance(((LikeExpression)incomingExpression).getRightExpression()) ||
                Column.class.isInstance(((LikeExpression)incomingExpression).getRightExpression()))) {
            LikeExpression likeExpression = (LikeExpression)incomingExpression;
            String stringValueLeftSide = SqlUtils.getStringValue(likeExpression.getLeftExpression());
            String stringValueRightSide = SqlUtils.getStringValue(likeExpression.getRightExpression());
            Document document = new Document("$regex", "^" + SqlUtils.replaceRegexCharacters(stringValueRightSide) + "$");
            if (likeExpression.isNot()) {
                throw new ParseException("NOT LIKE queries not supported");
            } else {
                document = new Document(stringValueLeftSide,document);
            }
            query.putAll(document);
        } else if(IsNullExpression.class.isInstance(incomingExpression)) {
            IsNullExpression isNullExpression = (IsNullExpression) incomingExpression;
            query.put(SqlUtils.getStringValue(isNullExpression.getLeftExpression()),new Document("$exists",isNullExpression.isNot()));
        } else if(InExpression.class.isInstance(incomingExpression)) {
            final InExpression inExpression = (InExpression) incomingExpression;
            final Expression leftExpression = ((InExpression) incomingExpression).getLeftExpression();
            final String leftExpressionAsString = SqlUtils.getStringValue(leftExpression);
            ObjectIdFunction objectIdFunction = SqlUtils.isObjectIdFunction(this, incomingExpression);

            if (objectIdFunction != null) {
                query.put(objectIdFunction.getColumn(), objectIdFunction.toDocument());
            } else {
                List<Object> objectList = Lists
                    .transform(((ExpressionList) inExpression.getRightItemsList()).getExpressions(),
                        new com.google.common.base.Function<Expression, Object>() {
                            @Override public Object apply(Expression expression) {
                                try {
                                    return parseExpression(new Document(), expression,
                                        leftExpression);
                                } catch (ParseException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

                if (Function.class.isInstance(leftExpression)) {
                    String mongoInFunction = inExpression.isNot() ? "$fnin" : "$fin";
                    query.put(mongoInFunction, new Document("function", parseExpression(new Document(), leftExpression, otherSide)).append("list", objectList));
                } else {
                    String mongoInFunction = inExpression.isNot() ? "$nin" : "$in";
                    Document doc = new Document();
                    List<Object> lobj = Arrays.asList(SqlUtils.nonFunctionToNode(leftExpression),(Object)objectList);
                	doc.put(mongoInFunction, lobj);
                	query.put("$expr", doc);

                }
            }
        } else if(AndExpression.class.isInstance(incomingExpression)) {
            handleAndOr("$and", (BinaryExpression)incomingExpression, query);
        } else if(OrExpression.class.isInstance(incomingExpression)) {
            handleAndOr("$or", (BinaryExpression)incomingExpression, query);
        } else if(Parenthesis.class.isInstance(incomingExpression)) {
            Parenthesis parenthesis = (Parenthesis) incomingExpression;
            Object expression = parseExpression(new Document(), parenthesis.getExpression(), null);
            if (parenthesis.isNot()) {	
                return new Document("$nor", Arrays.asList(expression));	
            }
            return expression;
        } else if (NotExpression.class.isInstance(incomingExpression) && otherSide == null) {
            Expression expression = ((NotExpression)incomingExpression).getExpression();

            /*if (Parenthesis.class.isInstance(expression)) {
                return new Document("$nor", Arrays.asList(parseExpression(query, expression, null)));
            }*/

            return new Document(SqlUtils.getStringValue(expression), new Document("$ne", true));
        } else if (Function.class.isInstance(incomingExpression)) {
            Function function = ((Function)incomingExpression);
            RegexFunction regexFunction = SqlUtils.isRegexFunction(incomingExpression);
            if (regexFunction != null) {
                Document regexDocument = new Document("$regex", regexFunction.getRegex());
                if (regexFunction.getOptions() != null) {
                    regexDocument.append("$options", regexFunction.getOptions());
                }
                query.put(regexFunction.getColumn(), regexDocument);
            } else {
                return recurseFunctions(query, function, defaultFieldType, fieldNameToFieldTypeMapping);
            }
        } else if (otherSide == null) {
            return new Document(SqlUtils.getStringValue(incomingExpression), true);
        } else {
            return SqlUtils.getValue(incomingExpression,otherSide, defaultFieldType, fieldNameToFieldTypeMapping);
        }
        return query;
    }

    protected Object recurseFunctions(Document query, Object object, FieldType defaultFieldType, Map<String, FieldType> fieldNameToFieldTypeMapping) throws ParseException {
        if (Function.class.isInstance(object)) {
            Function function = (Function)object;
            query.put("$" + FunctionProcessor.transcriptFunctionName(function.getName()), recurseFunctions(new Document(), function.getParameters(), defaultFieldType, fieldNameToFieldTypeMapping));
        } else if (ExpressionList.class.isInstance(object)) {
            ExpressionList expressionList = (ExpressionList)object;
            List<Object> objectList = new ArrayList<>();
            for (Expression expression : expressionList.getExpressions()) {
                objectList.add(recurseFunctions(new Document(), expression, defaultFieldType, fieldNameToFieldTypeMapping));
            }
            return objectList.size() == 1 ? objectList.get(0) : objectList;
        } else if (Expression.class.isInstance(object)) {
            return SqlUtils.getValue((Expression)object, null, defaultFieldType, fieldNameToFieldTypeMapping);
        }

        return query.isEmpty() ? null : query;
    }

    private void handleAndOr(String key, BinaryExpression incomingExpression, Document query) throws ParseException {
        final Expression leftExpression = incomingExpression.getLeftExpression();
        final Expression rightExpression = incomingExpression.getRightExpression();

        List result = flattenOrsOrAnds(new ArrayList(), leftExpression, leftExpression, rightExpression);

        if (result != null) {
            query.put(key, Lists.reverse(result));
        } else {
            query.put(key, Arrays.asList(parseExpression(new Document(), leftExpression, rightExpression),
                    parseExpression(new Document(), rightExpression, leftExpression)));
        }
    }

    private List flattenOrsOrAnds(List arrayList, Expression firstExpression, Expression leftExpression, Expression rightExpression) throws ParseException {
            if (firstExpression.getClass().isInstance(leftExpression) &&
                    isOrAndExpression(leftExpression) && !isOrAndExpression(rightExpression)) {
                Expression left = ((BinaryExpression)leftExpression).getLeftExpression();
                Expression right = ((BinaryExpression)leftExpression).getRightExpression();
                arrayList.add(parseExpression(new Document(), rightExpression, null));
                List result = flattenOrsOrAnds(arrayList, firstExpression, left, right);
                if (result != null) {
                    return arrayList;
                }
            } else if (isOrAndExpression(firstExpression) && !isOrAndExpression(leftExpression) && !isOrAndExpression(rightExpression)) {
                arrayList.add(parseExpression(new Document(), rightExpression, null));
                arrayList.add(parseExpression(new Document(), leftExpression, null));
                return arrayList;
            } else {
                return null;
            }
            return null;
    }

    private boolean isOrAndExpression(Expression expression) {
        return OrExpression.class.isInstance(expression) || AndExpression.class.isInstance(expression);
    }

}
