import type { MediatorHttp } from '@comunica/bus-http';
import type {
  IActionQuerySourceIdentifyHypermedia,
  IActorQuerySourceIdentifyHypermediaOutput,
  IActorQuerySourceIdentifyHypermediaArgs,
  IActorQuerySourceIdentifyHypermediaTest,
} from '@comunica/bus-query-source-identify-hypermedia';
import {
  ActorQuerySourceIdentifyHypermedia,
} from '@comunica/bus-query-source-identify-hypermedia';
import { QuerySourceSparql } from './QuerySourceSparql';

/**
 * A comunica SPARQL Query Source Identify Hypermedia Actor.
 */
export class ActorQuerySourceIdentifyHypermediaSparql extends ActorQuerySourceIdentifyHypermedia {
  public readonly mediatorHttp: MediatorHttp;
  public readonly checkUrlSuffix: boolean;
  public readonly forceHttpGet: boolean;
  public readonly cacheSize: number;
  public readonly bindMethod: BindMethod;

  public constructor(args: IActorQuerySourceIdentifyHypermediaSparqlArgs) {
    super(args, 'sparql');
  }

  public async testMetadata(
    action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<IActorQuerySourceIdentifyHypermediaTest> {
    if (!action.forceSourceType && !action.metadata.sparqlService &&
      !(this.checkUrlSuffix && action.url.endsWith('/sparql'))) {
      throw new Error(`Actor ${this.name} could not detect a SPARQL service description or URL ending on /sparql.`);
    }
    return { filterFactor: 1 };
  }

  public async run(action: IActionQuerySourceIdentifyHypermedia): Promise<IActorQuerySourceIdentifyHypermediaOutput> {
    this.logInfo(action.context, `Identified as sparql source: ${action.url}`);
    const source = new QuerySourceSparql(
      action.forceSourceType ? action.url : action.metadata.sparqlService || action.url,
      action.context,
      this.mediatorHttp,
      this.bindMethod,
      this.forceHttpGet,
      this.cacheSize,
    );
    return { source };
  }
}

export interface IActorQuerySourceIdentifyHypermediaSparqlArgs extends IActorQuerySourceIdentifyHypermediaArgs {
  /**
   * The HTTP mediator
   */
  mediatorHttp: MediatorHttp;
  /**
   * If URLs ending with '/sparql' should also be considered SPARQL endpoints.
   * @default {true}
   */
  checkUrlSuffix: boolean;
  /**
   * If non-update queries should be sent via HTTP GET instead of POST
   * @default {false}
   */
  forceHttpGet: boolean;
  /**
   * The cache size for COUNT queries.
   * @range {integer}
   * @default {1024}
   */
  cacheSize?: number;
  /**
   * The query operation for communicating bindings.
   * @default {values}
   */
  bindMethod: BindMethod;
}

export type BindMethod = 'values' | 'union' | 'filter';
