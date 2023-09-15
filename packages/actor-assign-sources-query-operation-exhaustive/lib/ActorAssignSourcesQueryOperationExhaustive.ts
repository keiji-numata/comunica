import type { IActionAssignSourcesQueryOperation, IActorAssignSourcesQueryOperationOutput,
  IActorAssignSourcesQueryOperationArgs } from '@comunica/bus-assign-sources-query-operation';
import { ActorAssignSourcesQueryOperation } from '@comunica/bus-assign-sources-query-operation';
import { KeysQueryOperation } from '@comunica/context-entries';
import type { IActorTest } from '@comunica/core';
import type { IQuerySourceWrapper } from '@comunica/types';
import { Algebra, Util } from 'sparqlalgebrajs';

/**
 * A comunica Exhaustive Assign Sources Query Operation Actor.
 */
export class ActorAssignSourcesQueryOperationExhaustive extends ActorAssignSourcesQueryOperation {
  public constructor(args: IActorAssignSourcesQueryOperationArgs) {
    super(args);
  }

  public async test(action: IActionAssignSourcesQueryOperation): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionAssignSourcesQueryOperation): Promise<IActorAssignSourcesQueryOperationOutput> {
    const sources: IQuerySourceWrapper[] = action.context.get(KeysQueryOperation.querySources) || [];
    if (sources.length === 0) {
      return { operation: action.operation };
    }
    if (sources.length === 1) {
      const shape = await sources[0].source.getSelectorShape(action.context);
      if (ActorAssignSourcesQueryOperation.doesShapeAcceptOperation(shape, action.operation)) {
        return { operation: ActorAssignSourcesQueryOperation.assignQueryOperationSource(action.operation, sources[0]) };
      }
    }
    return { operation: this.assignExhaustive(action.operation, sources) };
  }

  public assignExhaustive(operation: Algebra.Operation, sources: IQuerySourceWrapper[]): Algebra.Operation {
    return Util.mapOperation(operation, {
      [Algebra.types.PATTERN](subOperation, factory) {
        if (sources.length === 1) {
          return {
            result: ActorAssignSourcesQueryOperation.assignQueryOperationSource(factory.createPattern(
              subOperation.subject,
              subOperation.predicate,
              subOperation.object,
              subOperation.graph,
            ), sources[0]),
            recurse: false,
          };
        }
        return {
          result: factory.createUnion(sources.map(source => ActorAssignSourcesQueryOperation
            .assignQueryOperationSource(factory.createPattern(
              subOperation.subject,
              subOperation.predicate,
              subOperation.object,
              subOperation.graph,
            ), source))),
          recurse: false,
        };
      },
      [Algebra.types.LINK](subOperation, factory) {
        if (sources.length === 1) {
          return {
            result: ActorAssignSourcesQueryOperation
              .assignQueryOperationSource(factory.createLink(subOperation.iri), sources[0]),
            recurse: false,
          };
        }
        return {
          result: factory.createAlt(sources.map(source => ActorAssignSourcesQueryOperation
            .assignQueryOperationSource(factory.createLink(subOperation.iri), source))),
          recurse: false,
        };
      },
      [Algebra.types.SERVICE](subOperation) {
        return {
          result: subOperation,
          recurse: false,
        };
      },
    });
  }
}
