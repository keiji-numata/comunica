import exp = require('constants');
import type { IActionOptimizeQueryOperation, IActorOptimizeQueryOperationOutput, IActorOptimizeQueryOperationArgs } from '@comunica/bus-optimize-query-operation';
import { ActorOptimizeQueryOperation } from '@comunica/bus-optimize-query-operation';
import type { IActorTest } from '@comunica/core';
import type * as RDF from '@rdfjs/types';
import type { Factory } from 'sparqlalgebrajs';
import { Algebra, Util } from 'sparqlalgebrajs';
import { IActionContext } from '@comunica/types';

/**
 * A comunica Filter Pushdown Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationFilterPushdown extends ActorOptimizeQueryOperation {
  public constructor(args: IActorOptimizeQueryOperationArgs) {
    super(args);
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    const operation = Util.mapOperation(action.operation, {
      filter(op: Algebra.Filter, factory: Factory) {
        const variables = self.getExpressionVariables(op.expression);
        return {
          recurse: true,
          result: variables ? self.filterPushdown(op.expression, variables, op.input, factory, action.context) : op,
        };
      },
    });
    return { operation, context: action.context };
  }

  public getExpressionVariables(expression: Algebra.Expression): RDF.Variable[] | undefined {
    switch (expression.expressionType) {
      case Algebra.expressionTypes.AGGREGATE:
        // Not supported
        break;
      case Algebra.expressionTypes.EXISTENCE:
        // Not supported
        break;
      case Algebra.expressionTypes.WILDCARD:
        // Not supported
        break;
      case Algebra.expressionTypes.NAMED:
        return [];
      case Algebra.expressionTypes.OPERATOR:
        return expression.args.flatMap(arg => this.getExpressionVariables(arg)!);
      case Algebra.expressionTypes.TERM:
        if (expression.term.termType === 'Variable') {
          return [ expression.term ];
        }
        return [];
    }
  }

  public filterPushdown(
    expression: Algebra.Expression,
    expressionVariables: RDF.Variable[],
    operation: Algebra.Operation,
    factory: Factory,
    context: IActionContext,
  ): Algebra.Operation {
    /* eslint-disable no-case-declarations */
    switch (operation.type) {
      case Algebra.types.EXTEND:
        // Pass if the variable is not part of the expression
        if (!this.variablesOverlap([ operation.variable ], expressionVariables)) {
          return factory.createExtend(
            this.filterPushdown(expression, expressionVariables, operation.input, factory, context),
            operation.variable,
            operation.expression,
          );
        }
        return factory.createFilter(operation, expression);
      case Algebra.types.FILTER:
        // Always pass
        return factory.createFilter(
          this.filterPushdown(expression, expressionVariables, operation.input, factory, context),
          operation.expression,
        );
      case Algebra.types.JOIN:
        // Determine overlapping operations
        const fullyOverlapping: Algebra.Operation[] = [];
        const partiallyOverlapping: Algebra.Operation[] = [];
        const notOverlapping: Algebra.Operation[] = [];
        for (const input of operation.input) {
          const inputVariables = Util.inScopeVariables(input);
          if (this.allVariablesAreIn(expressionVariables, inputVariables)) {
            fullyOverlapping.push(input);
          } else if (this.variablesOverlap(expressionVariables, inputVariables)) {
            partiallyOverlapping.push(input);
          } else {
            notOverlapping.push(input);
          }
        }

        const joins: Algebra.Operation[] = [];
        this.logDebug(context, `Push down filter across join entries with ${fullyOverlapping.length} fully overlapping, ${partiallyOverlapping.length} partially overlapping, and ${notOverlapping.length} not overlapping`);
        if (fullyOverlapping.length > 0) {
          joins.push(factory.createJoin(fullyOverlapping
            .map(input => this.filterPushdown(expression, expressionVariables, input, factory, context))));
        }
        if (partiallyOverlapping.length > 0) {
          joins.push(factory.createFilter(factory.createJoin(partiallyOverlapping, false), expression));
        }
        if (notOverlapping.length > 0) {
          joins.push(...notOverlapping);
        }

        return joins.length === 1 ? joins[0] : factory.createJoin(joins);
      case Algebra.types.NOP:
        return operation;
      case Algebra.types.PROJECT:
        // Always pass
        return factory.createProject(
          this.filterPushdown(expression, expressionVariables, operation.input, factory, context),
          operation.variables,
        );
      case Algebra.types.UNION:
        // Pass only through to if all union inputs equal the variables range
        // TODO: we can probably handle more cases
        const equals = operation.input
          .map(input => this.allVariablesAreIn(expressionVariables, Util.inScopeVariables(input)));
        if (!equals.every(eq => eq)) {
          return factory.createFilter(operation, expression);
        }
        this.logDebug(context, `Push down filter across union entries`);
        return factory.createUnion(operation.input.map((input, index) => this
          .filterPushdown(expression, expressionVariables, input, factory, context)), false);
      case Algebra.types.VALUES:
        // Only keep filter if it overlaps with the variables
        if (this.variablesOverlap(operation.variables, expressionVariables)) {
          return factory.createFilter(operation, expression);
        }
        return operation;
      case Algebra.types.LEFT_JOIN:
      case Algebra.types.MINUS:
        // TODO: The above may be possible to support?
        return factory.createFilter(operation, expression);
      case Algebra.types.ALT:
      case Algebra.types.ASK:
      case Algebra.types.BGP:
      case Algebra.types.CONSTRUCT:
      case Algebra.types.DESCRIBE:
      case Algebra.types.DISTINCT:
      case Algebra.types.EXPRESSION:
      case Algebra.types.FROM:
      case Algebra.types.GRAPH:
      case Algebra.types.GROUP:
      case Algebra.types.INV:
      case Algebra.types.LINK:
      case Algebra.types.NPS:
      case Algebra.types.ONE_OR_MORE_PATH:
      case Algebra.types.ORDER_BY:
      case Algebra.types.PATTERN:
      case Algebra.types.REDUCED:
      case Algebra.types.SEQ:
      case Algebra.types.SERVICE:
      case Algebra.types.SLICE:
      case Algebra.types.PATH:
      case Algebra.types.ZERO_OR_MORE_PATH:
      case Algebra.types.ZERO_OR_ONE_PATH:
      case Algebra.types.COMPOSITE_UPDATE:
      case Algebra.types.DELETE_INSERT:
      case Algebra.types.LOAD:
      case Algebra.types.CLEAR:
      case Algebra.types.CREATE:
      case Algebra.types.DROP:
      case Algebra.types.ADD:
      case Algebra.types.MOVE:
      case Algebra.types.COPY:
        // Operations that do not support pushing down
        return factory.createFilter(operation, expression);
    }
    /* eslint-enable no-case-declarations */
  }

  // TODO: we can probably optimize the stuff below...

  public variablesOverlap(varsA: RDF.Variable[], varsB: RDF.Variable[]): boolean {
    return varsA.some(varA => varsB.some(varB => varA.equals(varB)));
  }

  public allVariablesAreIn(varsA: RDF.Variable[], varsB: RDF.Variable[]): boolean {
    return varsA.every(varA => varsB.some(varB => varA.equals(varB)));
  }
}
