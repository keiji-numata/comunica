import { ActorAssignSourcesQueryOperation } from '@comunica/bus-assign-sources-query-operation';
import type { IActionOptimizeQueryOperation, IActorOptimizeQueryOperationOutput, IActorOptimizeQueryOperationArgs } from '@comunica/bus-optimize-query-operation';
import { ActorOptimizeQueryOperation } from '@comunica/bus-optimize-query-operation';
import type { IActorTest } from '@comunica/core';
import type { IActionContext, IQuerySourceWrapper } from '@comunica/types';
import { Algebra, Factory } from 'sparqlalgebrajs';

const AF = new Factory();

/**
 * A comunica Group Sources Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationGroupSources extends ActorOptimizeQueryOperation {
  public constructor(args: IActorOptimizeQueryOperationArgs) {
    super(args);
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    return { operation: await this.groupOperation(action.operation, action.context), context: action.context };
  }

  public async groupOperation(operation: Algebra.Operation, context: IActionContext): Promise<Algebra.Operation> {
    // Return operation as-is if the operation already has a single source, or if the operation has no children.
    if (operation.metadata?.scopedSource || !('input' in operation)) {
      return operation;
    }

    // TODO: improve handling of paths; just do a switch-case over all types?

    // If operation has a single input, move source annotation upwards if the source can handle it.
    if (!Array.isArray(operation.input)) {
      const groupedInput = await this.groupOperation(operation.input, context);
      if (groupedInput.metadata?.scopedSource) {
        const source: IQuerySourceWrapper = groupedInput.metadata?.scopedSource;
        if (ActorAssignSourcesQueryOperation
          .doesShapeAcceptOperation(await source.source.getSelectorShape(context), operation)) {
          this.logDebug(context, `Hoist 1 source-specific operation into a single ${operation.type} operation for ${source.source.toString()}`);
          delete groupedInput.metadata?.scopedSource;
          operation = ActorAssignSourcesQueryOperation.assignQueryOperationSource(operation, source);
        }
      }
      return <Algebra.Operation> { ...operation, input: groupedInput };
    }

    // If operation has multiple inputs, cluster source annotations.
    const inputs: Algebra.Operation[] = await Promise.all(operation.input
      .map(subInput => this.groupOperation(subInput, context)));
    const clusters = this.clusterOperationsWithEqualSources(inputs);

    // If we just have a single cluster, move the source annotation upwards
    if (clusters.length <= 1) {
      const newInputs = clusters[0];
      const source = clusters[0][0].metadata?.scopedSource;
      return <Algebra.Operation> {
        ...await this.moveSourceAnnotationUpwardsIfPossible(operation, newInputs, source, context),
        input: newInputs,
      };
    }

    // If the number of clusters is equal to the number of original inputs, do nothing.
    if (clusters.length === inputs.length) {
      return <Algebra.Operation> { ...operation, input: inputs };
    }

    // If we have multiple clusters, created nested multi-operations
    /* eslint-disable no-case-declarations */
    // eslint-disable-next-line @typescript-eslint/switch-exhaustiveness-check
    switch (operation.type) {
      case Algebra.types.JOIN:
        let flatten = true;
        const nestedJoins = await Promise.all(clusters.map(async cluster => {
          const source = cluster[0].metadata?.scopedSource;
          const joined = await this
            .moveSourceAnnotationUpwardsIfPossible(AF.createJoin(cluster), cluster, source, context);
          if (joined.metadata?.scopedSource) {
            flatten = false;
          }
          return joined;
        }));
        return AF.createJoin(nestedJoins, flatten);
      case Algebra.types.UNION:
        const nestedUnions = await Promise.all(clusters.map(async cluster => {
          const source = cluster[0].metadata?.scopedSource;
          const unioned = await this
            .moveSourceAnnotationUpwardsIfPossible(AF.createUnion(cluster), cluster, source, context);
          if (unioned.metadata?.scopedSource) {
            flatten = false;
          }
          return unioned;
        }));
        return AF.createUnion(nestedUnions, true);
    }
    /* eslint-enable no-case-declarations */

    // In all other cases, error
    throw new Error(`Unsupported operation ${operation.type} detected while grouping sources`);
  }

  public clusterOperationsWithEqualSources(operationsIn: Algebra.Operation[]): Algebra.Operation[][] {
    // Operations can have a source, or no source at all
    const sourceOperations: Map<IQuerySourceWrapper, Algebra.Operation[]> = new Map();
    const sourcelessOperations: Algebra.Operation[] = [];

    // Cluster by source
    for (const operation of operationsIn) {
      const source: IQuerySourceWrapper = operation.metadata?.scopedSource;
      if (source) {
        if (!sourceOperations.has(source)) {
          sourceOperations.set(source, []);
        }
        sourceOperations.get(source)!.push(operation);
      } else {
        sourcelessOperations.push(operation);
      }
    }

    // Return clusters
    const clusters: Algebra.Operation[][] = [];
    if (sourcelessOperations.length > 0) {
      clusters.push(sourcelessOperations);
    }
    for (const [ source, operations ] of sourceOperations.entries()) {
      clusters.push(operations
        .map(operation => ActorAssignSourcesQueryOperation.assignQueryOperationSource(operation, source)));
    }
    return clusters;
  }

  public async moveSourceAnnotationUpwardsIfPossible<O extends Algebra.Operation>(
    operation: O,
    inputs: Algebra.Operation[],
    source: IQuerySourceWrapper,
    context: IActionContext,
  ): Promise<O> {
    if (source && ActorAssignSourcesQueryOperation
      .doesShapeAcceptOperation(await source.source.getSelectorShape(context), operation)) {
      this.logDebug(context, `Hoist ${inputs.length} source-specific operations into a single ${operation.type} operation for ${source.source.toString()}`);
      operation = ActorAssignSourcesQueryOperation.assignQueryOperationSource(operation, source);
      for (const input of inputs) {
        delete input.metadata?.scopedSource;
      }
    }
    return operation;
  }
}
