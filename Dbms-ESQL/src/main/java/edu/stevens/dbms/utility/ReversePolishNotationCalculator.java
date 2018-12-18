package edu.stevens.dbms.utility;

import com.sun.codemodel.*;
import edu.stevens.dbms.generator.QueryProcessor;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Pattern;


public class ReversePolishNotationCalculator {
    private JCodeModel jCodeModel = null;

    public  Map<String, String> attributesMethod = null;

    public ReversePolishNotationCalculator(JCodeModel jCodeModel,Map<String, String> attributesMethod) {
        this.jCodeModel = jCodeModel;
        this.attributesMethod= attributesMethod;
    }


    public JExpression performReversePolishNotation(String condition) {
        Stack<Object> evaluatorStack = this.conversionInfixPostfix(condition);

        Stack<Object> evaluatedStack = null;
        try {
            evaluatedStack = this.evaluateExpression(evaluatorStack, null, new Stack<Object>(), 0);
        } catch (ArithmeticException ae) {
            System.out.println("The infix notation cannot be evaluated " + ae.getMessage());
        }

        return (JExpression) evaluatedStack.pop();

    }

    private Stack<Object> evaluateExpression(Stack<Object> evaluatorStack, String operation, Stack<Object> poppedElement, int poppedElementCounter) {

        Object operand[] = new Object[2];
        int operandCounter = 0;
        Object currentChar;

        QueryProcessor queryProcessor = new QueryProcessor();

        while (evaluatorStack.size() != 0) {

            if ((evaluatorStack.peek() instanceof String &&
                    (Pattern.compile("[a-z A-Z0-9\\_\\~\"]").matcher((String) evaluatorStack.peek()).find()))

                    || evaluatorStack.peek() instanceof JExpression) {
                currentChar = evaluatorStack.pop();

                poppedElement.push(currentChar);
                operand[operandCounter] = currentChar;

                operandCounter++;
                poppedElementCounter++;

                if (evaluatorStack.size() == 0 && poppedElementCounter == 1) {
                    evaluatorStack.push(currentChar);
                    return evaluatorStack;
                }

            } else {
                currentChar = evaluatorStack.pop();
                poppedElement.push(currentChar);
                //[poppedElementCounter] = currentChar;
                poppedElementCounter++;
                operation = (String) currentChar;
                evaluatorStack = evaluateExpression(evaluatorStack, operation, poppedElement, poppedElementCounter);
                poppedElementCounter = 0;
                operandCounter = 0;
                if (evaluatorStack.size() == 1) {
                    return evaluatorStack;
                }
            }

            if (operandCounter == 2) {

                poppedElement.pop();
                poppedElement.pop();
                //  remove the element from stack

                JExpression result = null;


                if (operand[0] instanceof String) {

                    String dbEval[] = ((String) operand[0]).split("~", -1);

                    if (dbEval[0].matches("[A-Z]")) {

                        operand[0] = JExpr.ref("resultSet").invoke(this.attributesMethod.get(dbEval[1])).arg(dbEval[1]);
                    } else if ("PMF".equalsIgnoreCase(dbEval[0])) {
                        operand[0] = JExpr.ref("partialMfTable").invoke("get").arg(JExpr.ref("partialKeyToSearch")).invoke("get").arg(dbEval[1]);
                        operand[0] = (queryProcessor.accessStaticMethod(Double.class, "parseDouble").arg((JExpression) operand[0]));
                    } else if ("MF".equalsIgnoreCase(dbEval[0])) {
                        operand[0] = JOp.cond(
                                JExpr.ref("mfAttrMap").invoke("get").arg(dbEval[1]).ne(JExpr._null()),
                                JExpr.ref("mfAttrMap").invoke("get").arg(dbEval[1]), JExpr.lit("0"));

                        if (this.attributesMethod != null
                                && "getString".equalsIgnoreCase(this.attributesMethod.get(dbEval[1]))
                                ) {
                        } else {
                            operand[0] = (queryProcessor.accessStaticMethod(Double.class, "parseDouble").arg((JExpression) operand[0]));
                        }
                    } else if (dbEval[0].matches("^[0-9]*$")) {
                        operand[0] = JExpr.lit(Double.parseDouble(dbEval[0]));
                    } else if ("true".equalsIgnoreCase(dbEval[0])) {
                        operand[0] = JExpr.lit(true);
                    } else if ("false".equalsIgnoreCase(dbEval[0])) {
                        operand[0] = JExpr.lit(false);
                    } else {
                        operand[0] = JExpr.lit(dbEval[0]);
                    }
                }

                if (operand[1] instanceof String) {
                    String dbEval[] = ((String) operand[1]).split("~", -1);

                    if (dbEval[0].matches("[A-Z]")) {
                        operand[1] = JExpr.ref("resultSet").invoke(this.attributesMethod.get(dbEval[1])).arg(dbEval[1]);
                    } else if ("PMF".equalsIgnoreCase(dbEval[0])) {
                        operand[1] = JExpr.ref("partialMfTable").invoke("get").arg(JExpr.ref("partialKeyToSearch")).invoke("get").arg(dbEval[1]);
                        operand[1] = (queryProcessor.accessStaticMethod(Double.class, "parseDouble").arg((JExpression) operand[0]));
                    } else if ("MF".equalsIgnoreCase(dbEval[0])) {

                        operand[1] = JOp.cond(
                                JExpr.ref("mfAttrMap").invoke("get").arg(dbEval[1]).ne(JExpr._null()),
                                JExpr.ref("mfAttrMap").invoke("get").arg(dbEval[1]), JExpr.lit("0"));

                        if (this.attributesMethod != null
                                && "getString".equalsIgnoreCase(this.attributesMethod.get(dbEval[1]))
                                ) {
                        } else {
                            operand[1] = queryProcessor.accessStaticMethod(Double.class, "parseDouble").arg((JExpression) operand[1]);

                        }

                    } else if (dbEval[0].matches("^[0-9]*$")) {
                        operand[1] = JExpr.lit(Double.parseDouble(dbEval[0]));
                    } else if ("true".equalsIgnoreCase(dbEval[0])) {
                        operand[1] = JExpr.lit(true);
                    } else if ("false".equalsIgnoreCase(dbEval[0])) {
                        operand[1] = JExpr.lit(false);
                    } else {

                        operand[1] = JExpr.lit(dbEval[0]);
                    }
                }

                switch (operation) {

                    case "=":
                        JBlock nonStaticEqualComparison = new JBlock();
                        result = nonStaticEqualComparison.staticInvoke(jCodeModel.ref(String.class), "valueOf").arg(((JExpression) operand[0])).invoke("equalsIgnoreCase").arg(
                                jCodeModel.ref(String.class).staticInvoke("valueOf").arg(((JExpression) operand[1]))
                        );

                        //result = ((JExpression) operand[0]).invoke("equalsIgnoreCase").arg((JExpression) operand[1]);
                        break;
                    case "+":
                        result = ((JExpression) operand[0]).plus((JExpression) operand[1]);
                        break;
                    case "-":
                        result = ((JExpression) operand[1]).minus((JExpression) operand[0]);
                        break;
                    case "*":
                        result = ((JExpression) operand[0]).mul((JExpression) operand[1]);
                        break;
                    case "/":
                        result = ((JExpression) operand[1]).div((JExpression) operand[0]);
                        break;
                    case "^":
                        JBlock nonStaticBlock = new JBlock();
                        result = nonStaticBlock.staticInvoke(jCodeModel.ref(Math.class), "pow").arg(
                                jCodeModel.ref(Double.class).staticInvoke("parseDouble").arg(((JExpression) operand[1]))
                        ).arg(
                                jCodeModel.ref(Double.class).staticInvoke("parseDouble").arg(((JExpression) operand[0]))
                        );
                        break;
                    case "%":
                        result = ((JExpression) operand[1]).mod((JExpression) operand[0]);
                        break;
                    case "&":
                        result = ((JExpression) operand[0]).cand((JExpression) operand[1]);
                        break;
                    case "|":
                        result = ((JExpression) operand[0]).cor((JExpression) operand[1]);
                        break;
                    case ">":
                        result = ((JExpression) operand[1]).gt((JExpression) operand[0]);
                        break;
                    case "<":
                        result = ((JExpression) operand[1]).lt((JExpression) operand[0]);
                        break;
                    case ">=":
                        result = ((JExpression) operand[1]).gte((JExpression) operand[0]);
                        break;
                    case "=<":
                        result = ((JExpression) operand[1]).lte((JExpression) operand[0]);
                        break;
                    case "<>":
                        JBlock nonStaticNotEqualComparison = new JBlock();
                        result = nonStaticNotEqualComparison.staticInvoke(jCodeModel.ref(String.class), "valueOf").arg(((JExpression) operand[0])).invoke("equalsIgnoreCase").arg(
                                jCodeModel.ref(String.class).staticInvoke("valueOf").arg(((JExpression) operand[1]))
                        );
                        result = result.not();
                        break;
                }
                evaluatorStack.push(result);


                poppedElement.pop();

                while (poppedElement.size() != 0) {

                    evaluatorStack.push(poppedElement.pop());
                }
                return evaluatorStack;
            }
        }
        return evaluatorStack;

    }


    /*
     *   Code written for standardizing the input string for undisturbed evaluation and conversion of expression
     */
    private LinearQueue formatInputString(String inputString) {

        String conditions[] = inputString.trim().split(" ");
        LinearQueue linearQueue = new LinearQueue();

        for (String infixString : conditions) {
            linearQueue.enqueue(infixString);
        }
        return linearQueue;
    }

    /*
     *   Converting the Infix expression to Postfix expression
     *
     * */
    private Stack<Object> conversionInfixPostfix(String inputString) {
        Stack<Object> postfixStackPointer = new Stack<Object>();
        LinearQueue infixExpressionQueue = formatInputString(inputString);
        //infixExpressionQueue.displayQueue();
        LinearQueue postFixExpressionQueue = new LinearQueue();

        Node front = infixExpressionQueue.getFront();
        while (front != null) {
            String currentElement = front.getData();

            front = front.getNext();
            if ((Pattern.compile("[a-z A-Z0-9\\_\\~\"]").matcher((String) currentElement).find())) {
                postFixExpressionQueue.enqueue(currentElement);
            } else if ("(".equalsIgnoreCase(currentElement)) {
                postfixStackPointer.push(currentElement);
            } else if (")".equalsIgnoreCase(currentElement)) {
                // pop all the elements till we get first opening bracket
                String currentPoppedElement;
                do {
                    currentPoppedElement = (String) postfixStackPointer.pop();
                    if (!("(".equalsIgnoreCase(currentPoppedElement))) {
                        postFixExpressionQueue.enqueue(currentPoppedElement);
                    }
                }
                while (!"(".equalsIgnoreCase(currentPoppedElement));
            } else {
                while (postfixStackPointer.size() != 0 && precedence(currentElement) <= precedence((String) postfixStackPointer.peek())) {
                    postFixExpressionQueue.enqueue((String) postfixStackPointer.pop());
                }
                postfixStackPointer.push(currentElement);
            }
        }

        String currentElementPopped;

        while (postfixStackPointer.size() != 0) {
            currentElementPopped = (String) postfixStackPointer.pop();
            postFixExpressionQueue.enqueue(currentElementPopped);
        }

        front = postFixExpressionQueue.getFront();

        /* dequeuing the queue */
        while (front != null) {
            String currentElement = front.getData();
            postfixStackPointer.push(currentElement);
            front = front.getNext();
        }
        return postfixStackPointer;
    }

    private int precedence(String character) {
        switch (character) {
            case "+":
                return 2;
            case ">":
                return 1;
            case "<":
                return 1;
            case "-":
                return 2;
            case "*":
                return 3;
            case "/":
                return 3;
            case "%":
                return 3;
            case "^":
                return 4;
            case ">=":
                return 1;
            case "<=":
                return 1;
            case "=":
                return 1;
            default:
                return -1;
        }
    }
}


class Node {
    private String data;
    private Node next;

    public Node() {
        this.data = null;
        this.next = null;
    }

    public Node(String data) {
        this.data = data;
        this.next = null;
    }

    public Node(String data, Node next) {
        this.data = data;
        this.next = next;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Node getNext() {
        return next;
    }

    public void setNext(Node next) {
        this.next = next;
    }
}

class LinearQueue {

    private Node front;
    private Node rear;
    // size will be initialised to zero
    private int size;

    /*
     *   insert element into queue
     */
    public void enqueue(String data) {
        Node currentLinkedListNode = new Node(data, null);
        if (rear == null) {
            front = currentLinkedListNode;
            rear = currentLinkedListNode;
        } else {
            rear.setNext(currentLinkedListNode);
            rear = currentLinkedListNode;
        }
        // after adding the element increment the queue size
        size++;
    }




    public void displayQueue() {
        if (size != 0) {
            /*
             *  The queue always start from front pointer
             */
            Node pointerForDisplay = front;
            do {
                System.out.print(pointerForDisplay.getData());
                pointerForDisplay = pointerForDisplay.getNext();
                if (pointerForDisplay != null) {
                    System.out.print("");

                }
            } while (pointerForDisplay != null);
            System.out.println();
        }
    }

    public void peek() {
        if (front == null) {
            System.out.println("The queue is empty.");
        } else {
            System.out.println("Element of queue " + rear.getData());
        }
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public Node getFront() {
        return front;
    }

    public void setFront(Node front) {
        this.front = front;
    }

    public Node getRear() {
        return rear;
    }

    public void setRear(Node rear) {
        this.rear = rear;
    }
}


