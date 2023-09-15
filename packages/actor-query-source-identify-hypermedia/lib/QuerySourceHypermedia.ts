import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import type { IActorDereferenceRdfOutput } from '@comunica/bus-dereference-rdf';
import type { IActorRdfMetadataOutput } from '@comunica/bus-rdf-metadata';
import type { ILink } from '@comunica/bus-rdf-resolve-hypermedia-links';
import type {
  BindingsStream,
  FragmentSelectorShape,
  IActionContext,
  IAggregatedStore, IQueryBindingsOptions,
  IQuerySource, MetadataBindings,
} from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { TransformIterator } from 'asynciterator';
import { LRUCache } from 'lru-cache';
import { Readable } from 'readable-stream';
import type { Algebra } from 'sparqlalgebrajs';
import type { ISourceState } from './LinkedRdfSourcesAsyncRdfIterator';
import type { IMediatorArgs } from './MediatedLinkedRdfSourcesAsyncRdfIterator';
import { MediatedLinkedRdfSourcesAsyncRdfIterator } from './MediatedLinkedRdfSourcesAsyncRdfIterator';

export class QuerySourceHypermedia implements IQuerySource {
  public readonly firstUrl: string;
  public readonly forceSourceType?: string;
  public readonly mediators: IMediatorArgs;

  /**
   * A cache for source URLs to source states.
   */
  public sourcesState: LRUCache<string, Promise<ISourceState>>;
  public aggregatedStore: IAggregatedStore | undefined;

  private readonly cacheSize: number;
  private readonly maxIterators: number;

  public constructor(
    cacheSize: number,
    firstUrl: string,
    forceSourceType: string | undefined,
    maxIterators: number,
    aggregatedStore: IAggregatedStore | undefined,
    mediators: IMediatorArgs,
  ) {
    this.cacheSize = cacheSize;
    this.firstUrl = firstUrl;
    this.forceSourceType = forceSourceType;
    this.maxIterators = maxIterators;
    this.aggregatedStore = aggregatedStore;
    this.mediators = mediators;
    this.sourcesState = new LRUCache<string, Promise<ISourceState>>({ max: this.cacheSize });
  }

  public async getSelectorShape(context: IActionContext): Promise<FragmentSelectorShape> {
    const source = await this.getSourceCached({ url: this.firstUrl }, {}, context);
    return source.source.getSelectorShape(context);
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    // Optimized match with aggregated store if enabled and started.
    let aggregatedStore: IAggregatedStore | undefined;
    if (this.aggregatedStore && this.aggregatedStore.started && operation.type === 'pattern') {
      return new QuerySourceRdfJs(this.aggregatedStore).queryBindings(operation, context);
    }

    // Initialize the sources state on first call
    if (this.sourcesState.size === 0) {
      this.getSourceCached({ url: this.firstUrl }, {}, context)
        .catch(error => it.destroy(error));
    }

    const it: MediatedLinkedRdfSourcesAsyncRdfIterator = new MediatedLinkedRdfSourcesAsyncRdfIterator(
      this.cacheSize,
      operation,
      options,
      context,
      this.forceSourceType,
      this.firstUrl,
      this.maxIterators,
      (link, handledDatasets) => this.getSourceCached(link, handledDatasets, context),
      aggregatedStore,
      this.mediators,
    );
    if (aggregatedStore) {
      aggregatedStore.started = true;
    }

    return it;
  }

  public queryQuads(operation: Algebra.Construct, context: IActionContext): AsyncIterator<RDF.Quad> {
    return new TransformIterator(async() => {
      const source = await this.getSourceCached({ url: this.firstUrl }, {}, context);
      return source.source.queryQuads(operation, context);
    });
  }

  public async queryBoolean(operation: Algebra.Ask, context: IActionContext): Promise<boolean> {
    const source = await this.getSourceCached({ url: this.firstUrl }, {}, context);
    return await source.source.queryBoolean(operation, context);
  }

  public async queryVoid(operation: Algebra.Update, context: IActionContext): Promise<void> {
    const source = await this.getSourceCached({ url: this.firstUrl }, {}, context);
    return await source.source.queryVoid(operation, context);
  }

  /**
   * Resolve a source for the given URL.
   * @param link A source link.
   * @param handledDatasets A hash of dataset identifiers that have already been handled.
   * @param context The action context.
   */
  protected async getSource(
    link: ILink,
    handledDatasets: Record<string, boolean>,
    context: IActionContext,
  ): Promise<ISourceState> {
    // Include context entries from link
    if (link.context) {
      context = context.merge(link.context);
    }

    // Get the RDF representation of the given document
    let url = link.url;
    let quads: RDF.Stream;
    let metadata: Record<string, any>;
    try {
      const dereferenceRdfOutput: IActorDereferenceRdfOutput = await this.mediators.mediatorDereferenceRdf
        .mediate({ context, url });
      url = dereferenceRdfOutput.url;

      // Determine the metadata
      const rdfMetadataOutput: IActorRdfMetadataOutput = await this.mediators.mediatorMetadata.mediate(
        { context, url, quads: dereferenceRdfOutput.data, triples: dereferenceRdfOutput.metadata?.triples },
      );

      rdfMetadataOutput.data.on('error', () => {
        // Silence errors in the data stream,
        // as they will be emitted again in the metadata stream,
        // and will result in a promise rejection anyways.
        // If we don't do this, we end up with an unhandled error message
      });

      metadata = (await this.mediators.mediatorMetadataExtract.mediate({
        context,
        url,
        // The problem appears to be conflicting metadata keys here
        metadata: rdfMetadataOutput.metadata,
        headers: dereferenceRdfOutput.headers,
        requestTime: dereferenceRdfOutput.requestTime,
      })).metadata;
      quads = rdfMetadataOutput.data;

      // Optionally filter the resulting data
      if (link.transform) {
        quads = await link.transform(quads);
      }
    } catch (error: unknown) {
      // Make sure that dereference errors are only emitted once an actor really needs the read quads
      // This for example allows SPARQL endpoints that error on service description fetching to still be source-forcible
      quads = new Readable();
      quads.read = () => {
        setTimeout(() => quads.emit('error', error));
        return null;
      };
      ({ metadata } = await this.mediators.mediatorMetadataAccumulate.mediate({ context, mode: 'initialize' }));
    }

    // Aggregate all discovered quads into a store.
    this.aggregatedStore?.setBaseMetadata(<MetadataBindings> metadata, false);
    this.aggregatedStore?.import(quads);

    // Determine the source
    const { source, dataset } = await this.mediators.mediatorQuerySourceIdentifyHypermedia.mediate({
      context,
      forceSourceType: link.url === this.firstUrl ? this.forceSourceType : undefined,
      handledDatasets,
      metadata,
      quads,
      url,
    });

    if (dataset) {
      // Mark the dataset as applied
      // This is needed to make sure that things like QPF search forms are only applied once,
      // and next page links are followed after that.
      handledDatasets[dataset] = true;
    }

    return { link, source, metadata: <MetadataBindings> metadata, handledDatasets };
  }

  /**
   * Resolve a source for the given URL.
   * This will first try to retrieve the source from cache.
   * @param link A source ILink.
   * @param handledDatasets A hash of dataset identifiers that have already been handled.
   * @param context The action context.
   * @param aggregatedStore An optional aggregated store.
   */
  protected getSourceCached(
    link: ILink,
    handledDatasets: Record<string, boolean>,
    context: IActionContext,
  ): Promise<ISourceState> {
    let source = this.sourcesState.get(link.url);
    if (source) {
      return source;
    }
    source = this.getSource(link, handledDatasets, context);
    if (link.url === this.firstUrl || this.aggregatedStore === undefined) {
      this.sourcesState.set(link.url, source);
    }
    return source;
  }

  public toString(): string {
    return `QuerySourceHypermedia(${this.firstUrl})`;
  }
}
