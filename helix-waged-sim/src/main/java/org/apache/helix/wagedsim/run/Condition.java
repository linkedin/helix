package org.apache.helix.wagedsim.run;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * A condition over round stats, for example {@code skew.top.CU <= 1.10 and moves.replicas == 0}.
 * Grammar: {@code or := and ('or' and)*; and := unary ('and' unary)*;
 * unary := 'not' unary | '(' or ')' | stat op value}, with op one of {@code < <= > >= == !=} and
 * value a number or a quoted string. A comparison with a missing stat is false.
 */
public final class Condition {
  private final String _text;
  private final Node _root;

  private Condition(String text, Node root) {
    _text = text;
    _root = root;
  }

  public static Condition parse(String text) {
    Parser parser = new Parser(tokenize(text));
    Node root = parser.or();
    if (!parser.done()) {
      throw new IllegalArgumentException("Unexpected '" + parser.peek() + "' in condition: " + text);
    }
    return new Condition(text, root);
  }

  public boolean test(Map<String, Object> stats) {
    return _root.test(stats);
  }

  /** @return the stat names the condition reads */
  public Set<String> stats() {
    Set<String> names = new TreeSet<>();
    _root.collect(names);
    return names;
  }

  @Override
  public String toString() {
    return _text;
  }

  private interface Node {
    boolean test(Map<String, Object> stats);

    void collect(Set<String> names);
  }

  private static List<String> tokenize(String text) {
    List<String> tokens = new ArrayList<>();
    int i = 0;
    while (i < text.length()) {
      char c = text.charAt(i);
      if (Character.isWhitespace(c)) {
        i++;
      } else if (c == '(' || c == ')') {
        tokens.add(String.valueOf(c));
        i++;
      } else if (c == '<' || c == '>' || c == '=' || c == '!') {
        if (i + 1 < text.length() && text.charAt(i + 1) == '=') {
          tokens.add(text.substring(i, i + 2));
          i += 2;
        } else if (c == '<' || c == '>') {
          tokens.add(String.valueOf(c));
          i++;
        } else {
          throw new IllegalArgumentException("Use == or != in condition: " + text);
        }
      } else if (c == '"' || c == '\'') {
        int end = text.indexOf(c, i + 1);
        if (end < 0) {
          throw new IllegalArgumentException("Unclosed quote in condition: " + text);
        }
        tokens.add(text.substring(i, end + 1));
        i = end + 1;
      } else if (c == '&' && text.startsWith("&&", i)) {
        tokens.add("and");
        i += 2;
      } else if (c == '|' && text.startsWith("||", i)) {
        tokens.add("or");
        i += 2;
      } else {
        int start = i;
        while (i < text.length() && (Character.isLetterOrDigit(text.charAt(i))
            || "._-+:%/".indexOf(text.charAt(i)) >= 0)) {
          i++;
        }
        if (start == i) {
          throw new IllegalArgumentException("Unexpected '" + c + "' in condition: " + text);
        }
        tokens.add(text.substring(start, i));
      }
    }
    return tokens;
  }

  private static final class Parser {
    private final List<String> _tokens;
    private int _position;

    Parser(List<String> tokens) {
      _tokens = tokens;
    }

    boolean done() {
      return _position >= _tokens.size();
    }

    String peek() {
      return done() ? null : _tokens.get(_position);
    }

    String next() {
      if (done()) {
        throw new IllegalArgumentException("Condition ends too early");
      }
      return _tokens.get(_position++);
    }

    Node or() {
      Node left = and();
      while ("or".equalsIgnoreCase(peek())) {
        next();
        Node l = left;
        Node r = and();
        left = new Node() {
          @Override
          public boolean test(Map<String, Object> stats) {
            return l.test(stats) || r.test(stats);
          }

          @Override
          public void collect(Set<String> names) {
            l.collect(names);
            r.collect(names);
          }
        };
      }
      return left;
    }

    Node and() {
      Node left = unary();
      while ("and".equalsIgnoreCase(peek())) {
        next();
        Node l = left;
        Node r = unary();
        left = new Node() {
          @Override
          public boolean test(Map<String, Object> stats) {
            return l.test(stats) && r.test(stats);
          }

          @Override
          public void collect(Set<String> names) {
            l.collect(names);
            r.collect(names);
          }
        };
      }
      return left;
    }

    Node unary() {
      String token = peek();
      if ("not".equalsIgnoreCase(token)) {
        next();
        Node inner = unary();
        return new Node() {
          @Override
          public boolean test(Map<String, Object> stats) {
            return !inner.test(stats);
          }

          @Override
          public void collect(Set<String> names) {
            inner.collect(names);
          }
        };
      }
      if ("(".equals(token)) {
        next();
        Node inner = or();
        if (!")".equals(next())) {
          throw new IllegalArgumentException("Missing ')'");
        }
        return inner;
      }
      String stat = next();
      String op = next();
      String value = next();
      return comparison(stat, op, value);
    }

    private Node comparison(String stat, String op, String value) {
      if (!op.matches("<|<=|>|>=|==|!=")) {
        throw new IllegalArgumentException("Expected a comparison after '" + stat + "', got '" + op + "'");
      }
      boolean quoted = value.startsWith("\"") || value.startsWith("'");
      String literal = quoted ? value.substring(1, value.length() - 1) : value;
      Double number = null;
      if (!quoted) {
        try {
          number = Double.parseDouble(literal);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Expected a number or a quoted string, got '" + value + "'");
        }
      }
      Double expected = number;
      return new Node() {
        @Override
        public boolean test(Map<String, Object> stats) {
          Object actual = stats.get(stat);
          if (actual == null) {
            return false;
          }
          if (expected == null) {
            int cmp = actual.toString().compareTo(literal);
            return compare(cmp, op);
          }
          double numeric;
          try {
            numeric = actual instanceof Number ? ((Number) actual).doubleValue()
                : Double.parseDouble(actual.toString());
          } catch (NumberFormatException e) {
            return false;
          }
          if (Double.isNaN(numeric)) {
            return false;
          }
          return compare(Double.compare(numeric, expected), op);
        }

        @Override
        public void collect(Set<String> names) {
          names.add(stat);
        }
      };
    }

    private static boolean compare(int cmp, String op) {
      switch (op) {
        case "<":
          return cmp < 0;
        case "<=":
          return cmp <= 0;
        case ">":
          return cmp > 0;
        case ">=":
          return cmp >= 0;
        case "==":
          return cmp == 0;
        default:
          return cmp != 0;
      }
    }
  }
}
