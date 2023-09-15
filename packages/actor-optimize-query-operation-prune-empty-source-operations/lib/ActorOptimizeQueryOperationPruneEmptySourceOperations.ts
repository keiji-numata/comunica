import { ActorAssignSourcesQueryOperation } from '@comunica/bus-assign-sources-query-operation';
import type { IActionOptimizeQueryOperation, IActorOptimizeQueryOperationOutput, IActorOptimizeQueryOperationArgs } from '@comunica/bus-optimize-query-operation';
import { ActorOptimizeQueryOperation } from '@comunica/bus-optimize-query-operation';
import type { IActorTest } from '@comunica/core';
import type { IActionContext, IQuerySourceWrapper, MetadataBindings } from '@comunica/types';
import { DataFactory } from 'rdf-data-factory';
import { Algebra, Factory, Util } from 'sparqlalgebrajs';

const AF = new Factory();
const DF = new DataFactory();

/**
 * A comunica Prune Empty Source Operations Optimize Query Operation Actor.
 */
export class ActorOptimizeQueryOperationPruneEmptySourceOperations extends ActorOptimizeQueryOperation {
  private readonly useAskIfSupported: boolean;

  public constructor(args: IActorOptimizeQueryOperationPruneEmptySourceOperationsArgs) {
    super(args);
  }

  public async test(action: IActionOptimizeQueryOperation): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionOptimizeQueryOperation): Promise<IActorOptimizeQueryOperationOutput> {
    let operation = action.operation;

    // Collect all operations with source types
    // Only consider unions of patterns or alts of links, since these are created during exhaustive source assignment.
    const collectedOperations: (Algebra.Pattern | Algebra.Link)[] = [];
    Util.recurseOperation(operation, {
      [Algebra.types.UNION](subOperation) {
        for (const input of subOperation.input) {
          if (input.metadata?.scopedSource && input.type === 'pattern') {
            collectedOperations.push(input);
          }
        }
        return false;
      },
      [Algebra.types.ALT](subOperation) {
        for (const input of subOperation.input) {
          if (input.metadata?.scopedSource && input.type === 'link') {
            collectedOperations.push(input);
          }
        }
        return false;
      },
      [Algebra.types.PATTERN](subOperation) {
        collectedOperations.push(subOperation);
        return false;
      },
      [Algebra.types.LINK](subOperation) {
        collectedOperations.push(subOperation);
        return false;
      },
    });

    // Determine in an async manner whether or not these sources return non-empty results
    const emptyOperations: Set<Algebra.Operation> = new Set();
    await Promise.all(collectedOperations.map(async collectedOperation => {
      const checkOperation = collectedOperation.type === 'link' ?
        AF.createPattern(DF.variable('?s'), collectedOperation.iri, DF.variable('?o')) :
        collectedOperation;
      if (!await this.hasSourceResults(
        collectedOperation.metadata?.scopedSource,
        checkOperation,
        action.context,
      )) {
        emptyOperations.add(collectedOperation);
      }
    }));

    // Only perform next mapping if we have at least one empty operation
    this.logDebug(action.context, `Pruning ${emptyOperations.size} source-specific operations`);
    if (emptyOperations.size > 0) {
      // Rewrite operations by removing the empty children
      operation = Util.mapOperation(operation, {
        [Algebra.types.UNION](subOperation, factory) {
          // Determine which operations return non-empty results
          const nonEmptyInputs = subOperation.input.filter(input => !emptyOperations.has(input));

          // Remove empty operations
          if (nonEmptyInputs.length === subOperation.input.length) {
            return { result: subOperation, recurse: true };
          }
          if (nonEmptyInputs.length === 0) {
            return { result: factory.createNop(), recurse: false };
          }
          if (nonEmptyInputs.length === 1) {
            return { result: nonEmptyInputs[0], recurse: true };
          }
          return { result: factory.createUnion(nonEmptyInputs), recurse: true };
        },
        [Algebra.types.ALT](subOperation, factory) {
          // Determine which operations return non-empty results
          const nonEmptyInputs = subOperation.input.filter(input => !emptyOperations.has(input));

          // Remove empty operations
          if (nonEmptyInputs.length === subOperation.input.length) {
            return { result: subOperation, recurse: true };
          }
          if (nonEmptyInputs.length === 0) {
            return { result: factory.createNop(), recurse: false };
          }
          if (nonEmptyInputs.length === 1) {
            return { result: nonEmptyInputs[0], recurse: true };
          }
          return { result: factory.createAlt(nonEmptyInputs), recurse: true };
        },
        // TODO: also handle PATTERN and LINK
      });
    }

    return { operation, context: action.context };
  }

  public async hasSourceResults(
    source: IQuerySourceWrapper,
    input: Algebra.Operation,
    context: IActionContext,
  ): Promise<boolean> {
    if (this.useAskIfSupported) {
      const askOperation = AF.createAsk(input);
      if (ActorAssignSourcesQueryOperation
        .doesShapeAcceptOperation(await source.source.getSelectorShape(context), askOperation)) {
        return source.source.queryBoolean(askOperation, context);
      }
    }

    const bindingsStream = source.source.queryBindings(input, context);
    return new Promise((resolve, reject) => {
      bindingsStream.on('error', reject);
      bindingsStream.getProperty('metadata', (metadata: MetadataBindings) => {
        bindingsStream.destroy();
        resolve(metadata.cardinality.value > 0);
      });
    });
  }
}

export interface IActorOptimizeQueryOperationPruneEmptySourceOperationsArgs extends IActorOptimizeQueryOperationArgs {
  /**
   * If true, ASK queries will be sent to the source instead of COUNT queries to check emptiness for patterns.
   * This will only be done for sources that accept ASK queries.
   * @default {false}
   */
  useAskIfSupported: boolean;
}
