import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type {
  IActionQuerySourceIdentifyHypermedia,
  IActorQuerySourceIdentifyHypermediaOutput,
  IActorQuerySourceIdentifyHypermediaArgs,
  IActorQuerySourceIdentifyHypermediaTest,
} from '@comunica/bus-query-source-identify-hypermedia';
import {
  ActorQuerySourceIdentifyHypermedia,
} from '@comunica/bus-query-source-identify-hypermedia';
import type { MediatorRdfMetadata } from '@comunica/bus-rdf-metadata';
import type { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import type { IActionContext } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { QuerySourceQpf } from './QuerySourceQpf';

/**
 * A comunica QPF Query Source Identify Hypermedia Actor.
 */
export class ActorQuerySourceIdentifyHypermediaQpf extends ActorQuerySourceIdentifyHypermedia
  implements IActorQuerySourceIdentifyHypermediaQpfArgs {
  public readonly mediatorMetadata: MediatorRdfMetadata;
  public readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly subjectUri: string;
  public readonly predicateUri: string;
  public readonly objectUri: string;
  public readonly graphUri?: string;
  public readonly bindingsRestrictedChunkSize: number;
  public constructor(args: IActorQuerySourceIdentifyHypermediaQpfArgs) {
    super(args, 'qpf');
  }

  public async test(action: IActionQuerySourceIdentifyHypermedia): Promise<IActorQuerySourceIdentifyHypermediaTest> {
    if (action.forceSourceType && (action.forceSourceType !== 'qpf' && action.forceSourceType !== 'brtpf')) {
      throw new Error(`Actor ${this.name} is not able to handle source type ${action.forceSourceType}.`);
    }
    return this.testMetadata(action);
  }

  public async testMetadata(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<IActorQuerySourceIdentifyHypermediaTest> {
    const { searchForm } = this.createSource(
      action.url,
      action.metadata,
      action.context,
      action.forceSourceType === 'brtpf',
    );
    if (action.handledDatasets && action.handledDatasets[searchForm.dataset]) {
      throw new Error(`Actor ${this.name} can only be applied for the first page of a QPF dataset.`);
    }
    return { filterFactor: 1 };
  }

  /**
   * Look for the search form
   * @param {IActionRdfResolveHypermedia} action the metadata to look for the form.
   * @return {Promise<IActorRdfResolveHypermediaOutput>} A promise resolving to a hypermedia form.
   */
  public async run(action: IActionQuerySourceIdentifyHypermedia): Promise<IActorQuerySourceIdentifyHypermediaOutput> {
    this.logInfo(action.context, `Identified as qpf source: ${action.url}`);
    const source = this.createSource(
      action.url,
      action.metadata,
      action.context,
      action.forceSourceType === 'brtpf',
      action.quads,
    );
    return { source, dataset: source.searchForm.dataset };
  }

  protected createSource(
    url: string,
    metadata: Record<string, any>,
    context: IActionContext,
    bindingsRestricted: boolean,
    quads?: RDF.Stream,
  ): QuerySourceQpf {
    return new QuerySourceQpf(
      this.mediatorMetadata,
      this.mediatorMetadataExtract,
      this.mediatorDereferenceRdf,
      this.subjectUri,
      this.predicateUri,
      this.objectUri,
      this.graphUri,
      this.bindingsRestrictedChunkSize,
      url,
      metadata,
      context,
      bindingsRestricted,
      quads,
    );
  }
}

export interface IActorQuerySourceIdentifyHypermediaQpfArgs extends IActorQuerySourceIdentifyHypermediaArgs {
  /**
   * The metadata mediator
   */
  mediatorMetadata: MediatorRdfMetadata;
  /**
   * The metadata extract mediator
   */
  mediatorMetadataExtract: MediatorRdfMetadataExtract;
  /**
   * The RDF dereference mediator
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The URI that should be interpreted as subject URI
   * @default {http://www.w3.org/1999/02/22-rdf-syntax-ns#subject}
   */
  subjectUri: string;
  /**
   * The URI that should be interpreted as predicate URI
   * @default {http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate}
   */
  predicateUri: string;
  /**
   * The URI that should be interpreted as object URI
   * @default {http://www.w3.org/1999/02/22-rdf-syntax-ns#object}
   */
  objectUri: string;
  /**
   * The URI that should be interpreted as graph URI
   * @default {http://www.w3.org/ns/sparql-service-description#graph}
   */
  graphUri?: string;
  /**
   * The maximum amount of bindings to send in once request for a bindings-restricted interface.
   * @default {64}
   */
  bindingsRestrictedChunkSize: number;
}
