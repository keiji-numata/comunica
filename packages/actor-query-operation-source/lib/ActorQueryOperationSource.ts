import type { IActionQueryOperation, IActorQueryOperationArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperation } from '@comunica/bus-query-operation';
import type { IActorTest } from '@comunica/core';
import type {
  IQueryOperationResult,
  BindingsStream,
  MetadataBindings,
  IQuerySourceWrapper,
} from '@comunica/types';
import { Algebra } from 'sparqlalgebrajs';

/**
 * A comunica Source Query Operation Actor.
 */
export class ActorQueryOperationSource extends ActorQueryOperation {
  public constructor(args: IActorQueryOperationArgs) {
    super(args);
  }

  public async test(action: IActionQueryOperation): Promise<IActorTest> {
    if (!action.operation.metadata?.scopedSource) {
      throw new Error(`${this.name} requires an operation with a scopedSource metadata field, received type ${action.operation.type}.`);
    }
    return { httpRequests: 1 };
  }

  public async run(action: IActionQueryOperation): Promise<IQueryOperationResult> {
    const sourceWrapper: IQuerySourceWrapper = action.operation.metadata?.scopedSource;

    switch (action.operation.type) {
      case Algebra.types.CONSTRUCT:
        throw new Error('Constructs are not supported yet'); // TODO
      case Algebra.types.ASK:
        throw new Error('Asks are not supported yet'); // TODO
      case Algebra.types.COMPOSITE_UPDATE:
        throw new Error('Updates are not supported yet'); // TODO
      default: {
        const bindingsStream = sourceWrapper.source.queryBindings(
          action.operation,
          sourceWrapper.context ? action.context.merge(sourceWrapper.context) : action.context,
        );
        const metadata = ActorQueryOperationSource.getMetadata(bindingsStream);
        return {
          type: 'bindings',
          bindingsStream,
          metadata,
        };
      }
    }
  }

  protected static getMetadata(data: BindingsStream): () => Promise<MetadataBindings> {
    return ActorQueryOperation.cachifyMetadata(() => new Promise<Record<string, any>>((resolve, reject) => {
      data.getProperty('metadata', (metadata: Record<string, any>) => resolve(metadata));
      data.on('error', reject);
    }).then(metadataRaw => {
      if (!('canContainUndefs' in metadataRaw)) {
        metadataRaw.canContainUndefs = false;
      }
      return ActorQueryOperationSource.validateMetadata(metadataRaw);
    }));
  }

  /**
   * Ensure that the given raw metadata object contains all required metadata entries.
   * @param metadataRaw A raw metadata object.
   */
  public static validateMetadata(metadataRaw: Record<string, any>): MetadataBindings {
    for (const key of [ 'cardinality', 'canContainUndefs', 'variables' ]) {
      if (!(key in metadataRaw)) {
        throw new Error(`Invalid metadata: missing ${key} in ${JSON.stringify(metadataRaw)}`);
      }
    }
    return <MetadataBindings> metadataRaw;
  }
}
