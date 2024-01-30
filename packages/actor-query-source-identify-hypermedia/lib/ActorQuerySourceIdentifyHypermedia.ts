import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import { ActorQuerySourceIdentify } from '@comunica/bus-query-source-identify';
import type { IActionQuerySourceIdentify, IActorQuerySourceIdentifyOutput,
  IActorQuerySourceIdentifyArgs } from '@comunica/bus-query-source-identify';
import type { MediatorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import type { MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import type { MediatorRdfMetadataAccumulate } from '@comunica/bus-rdf-metadata-accumulate';
import type { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import type { MediatorRdfResolveHypermediaLinks } from '@comunica/bus-rdf-resolve-hypermedia-links';
import type { MediatorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { KeysQuerySourceIdentify } from '@comunica/context-entries';
import { ActionContext } from '@comunica/core';
import type { IActorTest } from '@comunica/core';
import type { IAggregatedStore, MetadataBindings } from '@comunica/types';
import { QuerySourceHypermedia } from './QuerySourceHypermedia';
import { StreamingStoreMetadata } from './StreamingStoreMetadata';

/**
 * A comunica Hypermedia Query Source Identify Actor.
 */
export class ActorQuerySourceIdentifyHypermedia extends ActorQuerySourceIdentify {
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly mediatorMetadata: MediatorRdfMetadata;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;
  public readonly mediatorMetadataAccumulate: MediatorRdfMetadataAccumulate;
  public readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
  public readonly mediatorRdfResolveHypermediaLinks: MediatorRdfResolveHypermediaLinks;
  public readonly mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
  public readonly cacheSize: number;
  public readonly maxIterators: number;
  public readonly aggregateStore: boolean;

  public constructor(args: IActorQuerySourceIdentifyHypermediaArgs) {
    super(args);
  }

  public async test(action: IActionQuerySourceIdentify): Promise<IActorTest> {
    if (typeof action.querySourceUnidentified.value !== 'string') {
      throw new Error(`${this.name} requires a single query source with a URL value to be present in the context.`);
    }
    return true;
  }

  public async run(action: IActionQuerySourceIdentify): Promise<IActorQuerySourceIdentifyOutput> {
    const url = <string> action.querySourceUnidentified.value;

    // Create an aggregate store if enabled
    let aggregatedStore: IAggregatedStore | undefined;
    if (this.aggregateStore) {
      const aggregatedStores: Map<string, IAggregatedStore> | undefined = action.context
        .get(KeysQuerySourceIdentify.hypermediaSourcesAggregatedStores);
      if (aggregatedStores) {
        aggregatedStore = aggregatedStores.get(url);
        if (!aggregatedStore) {
          aggregatedStore = new StreamingStoreMetadata(
            undefined,
            async(accumulatedMetadata, appendingMetadata) => <MetadataBindings>
              (await this.mediatorMetadataAccumulate.mediate({
                mode: 'append',
                accumulatedMetadata,
                appendingMetadata,
                context: action.context,
              })).metadata,
          );
          aggregatedStores.set(url, aggregatedStore);
        }
      }
    }

    return {
      querySource: {
        source: new QuerySourceHypermedia(
          this.cacheSize,
          url,
          action.querySourceUnidentified.type,
          this.maxIterators,
          aggregatedStore,
          {
            mediatorMetadata: this.mediatorMetadata,
            mediatorMetadataExtract: this.mediatorMetadataExtract,
            mediatorMetadataAccumulate: this.mediatorMetadataAccumulate,
            mediatorDereferenceRdf: this.mediatorDereferenceRdf,
            mediatorQuerySourceIdentifyHypermedia: this.mediatorQuerySourceIdentifyHypermedia,
            mediatorRdfResolveHypermediaLinks: this.mediatorRdfResolveHypermediaLinks,
            mediatorRdfResolveHypermediaLinksQueue: this.mediatorRdfResolveHypermediaLinksQueue,
          },
          warningMessage => this.logWarn(action.context, warningMessage),
        ),
        context: action.querySourceUnidentified.context || new ActionContext(),
      },
    };
  }
}

export interface IActorQuerySourceIdentifyHypermediaArgs extends IActorQuerySourceIdentifyArgs {
  /**
   * The maximum number of entries in the LRU cache, set to 0 to disable.
   * @range {integer}
   * @default {100}
   */
  cacheSize: number;
  /**
   * The maximum number of links that can be followed in parallel.
   * @default {64}
   */
  maxIterators: number;
  /**
   * If all discovered quads across all links from a seed source should be indexed in an aggregated store,
   * to speed up later calls.
   * This should only be used for sources without filter factor.
   * @default {false}
   */
  aggregateStore: boolean;
  /**
   * The RDF dereference mediator
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The metadata mediator
   */
  mediatorMetadata: MediatorRdfMetadata;
  /**
   * The metadata extract mediator
   */
  mediatorMetadataExtract: MediatorRdfMetadataExtract;
  /**
   * The metadata accumulate mediator
   */
  mediatorMetadataAccumulate?: MediatorRdfMetadataAccumulate;
  /**
   * The hypermedia resolve mediator
   */
  mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
  /**
   * The hypermedia links resolve mediator
   */
  mediatorRdfResolveHypermediaLinks: MediatorRdfResolveHypermediaLinks;
  /**
   * The hypermedia links queue resolve mediator
   */
  mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
}
