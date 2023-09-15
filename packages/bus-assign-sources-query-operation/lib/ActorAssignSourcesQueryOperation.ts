import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';
import type { FragmentSelectorShape, IQuerySourceWrapper } from '@comunica/types';
import type { Algebra } from 'sparqlalgebrajs';

/**
 * A comunica actor for assign-sources-query-operation events.
 *
 * Actor types:
 * * Input:  IActionAssignSourcesQueryOperation:      An incoming SPARQL operation.
 * * Test:   <none>
 * * Output: IActorAssignSourcesQueryOperationOutput: An outgoing SPARQL operation where sources are assigned.
 *
 * @see IActionAssignSourcesQueryOperation
 * @see IActorAssignSourcesQueryOperationOutput
 */
export abstract class ActorAssignSourcesQueryOperation
  extends Actor<IActionAssignSourcesQueryOperation, IActorTest, IActorAssignSourcesQueryOperationOutput> {
  /**
  * @param args - @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
  */
  public constructor(args: IActorAssignSourcesQueryOperationArgs) {
    super(args);
  }

  public static assignQueryOperationSource<O extends Algebra.Operation>(operation: O, source: IQuerySourceWrapper): O {
    operation = { ...operation };
    operation.metadata = operation.metadata || {};
    operation.metadata.scopedSource = source;
    return operation;
  }

  // TODO: move this elsewhere?
  public static doesShapeAcceptOperation(
    shape: FragmentSelectorShape,
    operation: Algebra.Operation,
    options?: { joinBindings?: boolean; filterBindings?: boolean },
  ): boolean {
    if (shape.type === 'conjunction') {
      return shape.children.every(child => this.doesShapeAcceptOperation(child, operation, options));
    }
    if (shape.type === 'disjunction') {
      return shape.children.some(child => this.doesShapeAcceptOperation(child, operation, options));
    }
    if (shape.type === 'arity') {
      return this.doesShapeAcceptOperation(shape.child, operation, options);
    }

    if ((options?.joinBindings && !shape.joinBindings) || options?.filterBindings && !shape.filterBindings) {
      return false;
    }

    if (shape.operation.operationType === 'type') {
      return shape.operation.type === 'project' || shape.operation.type === operation.type;
    }
    return shape.operation.pattern.type === operation.type;
  }
}

export interface IActionAssignSourcesQueryOperation extends IAction {
  operation: Algebra.Operation;
}

export interface IActorAssignSourcesQueryOperationOutput extends IActorOutput {
  operation: Algebra.Operation;
}

export type IActorAssignSourcesQueryOperationArgs = IActorArgs<
IActionAssignSourcesQueryOperation, IActorTest, IActorAssignSourcesQueryOperationOutput>;

export type MediatorAssignSourcesQueryOperation = Mediate<
IActionAssignSourcesQueryOperation, IActorAssignSourcesQueryOperationOutput>;
