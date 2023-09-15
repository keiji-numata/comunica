import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { MediatorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import type { MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import type { MediatorRdfMetadataAccumulate } from '@comunica/bus-rdf-metadata-accumulate';
import type { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import type { ILink,
  MediatorRdfResolveHypermediaLinks } from '@comunica/bus-rdf-resolve-hypermedia-links';
import type { ILinkQueue,
  MediatorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import type { IActionContext, IAggregatedStore } from '@comunica/types';
import type { Algebra } from 'sparqlalgebrajs';
import type { SourceStateGetter } from './LinkedRdfSourcesAsyncRdfIterator';
import { LinkedRdfSourcesAsyncRdfIterator } from './LinkedRdfSourcesAsyncRdfIterator';
import { BindingsStream, IQueryBindingsOptions, MetadataBindings } from '@comunica/types';

/**
 * An quad iterator that can iterate over consecutive RDF sources
 * that are determined using the rdf-resolve-hypermedia-links bus.
 *
 * @see LinkedRdfSourcesAsyncRdfIterator
 */
export class MediatedLinkedRdfSourcesAsyncRdfIterator extends LinkedRdfSourcesAsyncRdfIterator {
  private readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  private readonly mediatorMetadata: MediatorRdfMetadata;
  private readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;
  private readonly mediatorMetadataAccumulate: MediatorRdfMetadataAccumulate;
  private readonly mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
  private readonly mediatorRdfResolveHypermediaLinks: MediatorRdfResolveHypermediaLinks;
  private readonly mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
  private readonly forceSourceType?: string;
  private readonly handledUrls: Record<string, boolean>;
  private readonly aggregatedStore: IAggregatedStore | undefined;
  private linkQueue: Promise<ILinkQueue> | undefined;
  private wasForcefullyClosed = false;

  public constructor(
    cacheSize: number,
    operation: Algebra.Operation,
    queryBindingsOptions: IQueryBindingsOptions | undefined,
    context: IActionContext,
    forceSourceType: string | undefined,
    firstUrl: string,
    maxIterators: number,
    sourceStateGetter: SourceStateGetter,
    aggregatedStore: IAggregatedStore | undefined,
    mediators: IMediatorArgs,
  ) {
    super(
      cacheSize,
      operation,
      queryBindingsOptions,
      context,
      firstUrl,
      maxIterators,
      sourceStateGetter,
      // Buffersize must be infinite for an aggregated store because it must keep filling until there are no more
      // derived iterators in the aggregated store.
      aggregatedStore ? { maxBufferSize: Number.POSITIVE_INFINITY } : undefined,
    );
    this.forceSourceType = forceSourceType;
    this.mediatorDereferenceRdf = mediators.mediatorDereferenceRdf;
    this.mediatorMetadata = mediators.mediatorMetadata;
    this.mediatorMetadataExtract = mediators.mediatorMetadataExtract;
    this.mediatorMetadataAccumulate = mediators.mediatorMetadataAccumulate;
    this.mediatorQuerySourceIdentifyHypermedia = mediators.mediatorQuerySourceIdentifyHypermedia;
    this.mediatorRdfResolveHypermediaLinks = mediators.mediatorRdfResolveHypermediaLinks;
    this.mediatorRdfResolveHypermediaLinksQueue = mediators.mediatorRdfResolveHypermediaLinksQueue;
    this.handledUrls = { [firstUrl]: true };
    this.aggregatedStore = aggregatedStore;
  }

  // Mark the aggregated store as ended once we trigger the closing or destroying of this iterator.
  // We don't override _end, because that would mean that we have to wait
  // until the buffer of this iterator must be fully consumed, which will not always be the case.

  public close(): void {
    this.getLinkQueue()
      .then(linkQueue => {
        if (this.isCloseable(linkQueue)) {
          this.aggregatedStore?.end();
          super.close();
        } else {
          this.wasForcefullyClosed = true;
        }
      })
      .catch(error => super.destroy(error));
  }

  public destroy(cause?: Error): void {
    this.getLinkQueue()
      .then(linkQueue => {
        if (this.isCloseable(linkQueue)) {
          this.aggregatedStore?.end();
          super.destroy(cause);
        } else {
          this.wasForcefullyClosed = true;
        }
      })
      .catch(error => super.destroy(error));
  }

  protected isCloseable(linkQueue: ILinkQueue): boolean {
    return (this.wasForcefullyClosed || linkQueue.isEmpty()) && !this.areIteratorsRunning();
  }

  protected override canStartNewIterator(): boolean {
    // Also allow sub-iterators to be started if the aggregated store has at least one running iterator.
    // We need this because there are cases where these running iterators will be consumed before this linked iterator.
    return !this.wasForcefullyClosed &&
      (this.aggregatedStore && this.aggregatedStore.hasRunningIterators()) || super.canStartNewIterator();
  }

  protected override isRunning(): boolean {
    // Same as above
    return (this.aggregatedStore && this.aggregatedStore.hasRunningIterators()) || !this.done;
  }

  public getLinkQueue(): Promise<ILinkQueue> {
    if (!this.linkQueue) {
      this.linkQueue = this.mediatorRdfResolveHypermediaLinksQueue
        .mediate({ firstUrl: this.firstUrl, context: this.context })
        .then(result => result.linkQueue);
    }
    return this.linkQueue;
  }

  protected async getSourceLinks(metadata: Record<string, any>): Promise<ILink[]> {
    try {
      const { links } = await this.mediatorRdfResolveHypermediaLinks.mediate({ context: this.context, metadata });

      // Filter URLs to avoid cyclic next-page loops
      return links.filter(link => {
        if (this.handledUrls[link.url]) {
          return false;
        }
        this.handledUrls[link.url] = true;
        return true;
      });
    } catch {
      // No next URLs may be available, for example when we've reached the end of a Hydra next-page sequence.
      return [];
    }
  }

  public async accumulateMetadata(
    accumulatedMetadata: MetadataBindings,
    appendingMetadata: MetadataBindings,
  ): Promise<MetadataBindings> {
    return <MetadataBindings> (await this.mediatorMetadataAccumulate.mediate({
      mode: 'append',
      accumulatedMetadata,
      appendingMetadata,
      context: this.context,
    })).metadata;
  }

  protected updateMetadata(metadataNew: MetadataBindings): void {
    super.updateMetadata(metadataNew);
    this.aggregatedStore?.setBaseMetadata(metadataNew, true);
  }
}

export interface IMediatorArgs {
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  mediatorMetadata: MediatorRdfMetadata;
  mediatorMetadataExtract: MediatorRdfMetadataExtract;
  mediatorMetadataAccumulate: MediatorRdfMetadataAccumulate;
  mediatorQuerySourceIdentifyHypermedia: MediatorQuerySourceIdentifyHypermedia;
  mediatorRdfResolveHypermediaLinks: MediatorRdfResolveHypermediaLinks;
  mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
}
