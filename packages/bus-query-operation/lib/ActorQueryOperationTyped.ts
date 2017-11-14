import {IActorArgs, IActorTest} from "@comunica/core";
import {AsyncIterator, BufferedIterator, SimpleTransformIteratorOptions} from "asynciterator";
import {Algebra} from "sparqlalgebrajs";
import {ActorQueryOperation, IActionQueryOperation, IActorQueryOperationOutput} from "./ActorQueryOperation";

/**
 * A base implementation for query operation actors for a specific operation type.
 */
export abstract class ActorQueryOperationTyped<O extends Algebra.Operation> extends ActorQueryOperation
  implements IActorQueryOperationTypedArgs {

  public readonly operationName: string;

  constructor(args: IActorArgs<IActionQueryOperation, IActorTest, IActorQueryOperationOutput>) {
    super(args);
    if (!this.operationName) {
      throw new Error('A valid "operationName" argument must be provided.');
    }
  }

  public async test(action: IActionQueryOperation): Promise<IActorTest> {
    if (action.operation && action.operation.type !== this.operationName) {
      throw new Error('Actor ' + this.name + ' only supports ' + this.operationName + ' operations, but got '
        + action.operation.type);
    }
    return true;
  }

  public async run(action: IActionQueryOperation): Promise<IActorQueryOperationOutput> {
    const operation: O = <O> action.operation;
    return this.runOperation(operation, action.context);
  }

  protected abstract runOperation(operation: O, context?: {[id: string]: any}): Promise<IActorQueryOperationOutput>;

}

export interface IActorQueryOperationTypedArgs extends
  IActorArgs<IActionQueryOperation, IActorTest, IActorQueryOperationOutput> {
  operationName: string;
}
