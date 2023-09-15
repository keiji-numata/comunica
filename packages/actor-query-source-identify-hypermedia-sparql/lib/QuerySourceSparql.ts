import { BindingsFactory } from '@comunica/bindings-factory';
import type { MediatorHttp } from '@comunica/bus-http';
import type {
  IQuerySource,
  BindingsStream,
  IActionContext,
  FragmentSelectorShape,
  Bindings, MetadataBindings,
} from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import { wrap, TransformIterator, AsyncIterator } from 'asynciterator';
import { SparqlEndpointFetcher } from 'fetch-sparql-endpoint';
import { LRUCache } from 'lru-cache';
import { DataFactory } from 'rdf-data-factory';
import { Algebra, Factory, toSparql, Util } from 'sparqlalgebrajs';
import type { BindMethod } from './ActorQuerySourceIdentifyHypermediaSparql';
import { IQueryBindingsOptions } from '@comunica/types';

const stringifyStream = require('stream-to-string');

const AF = new Factory();
const DF = new DataFactory<RDF.BaseQuad>();
const BF = new BindingsFactory();
const VAR_COUNT = DF.variable('count');

export class QuerySourceSparql implements IQuerySource {
  protected readonly selectorShape: FragmentSelectorShape = {
    type: 'disjunction',
    children: [
      {
        type: 'operation',
        operation: { operationType: 'type', type: Algebra.types.PROJECT },
        joinBindings: true,
      },
      {
        type: 'operation',
        operation: { operationType: 'type', type: Algebra.types.CONSTRUCT },
      },
      {
        type: 'operation',
        operation: { operationType: 'type', type: Algebra.types.DESCRIBE },
      },
      {
        type: 'operation',
        operation: { operationType: 'type', type: Algebra.types.ASK },
      },
      {
        type: 'operation',
        operation: { operationType: 'type', type: Algebra.types.COMPOSITE_UPDATE },
      },
    ],
  };

  private readonly url: string;
  private readonly context: IActionContext;
  private readonly mediatorHttp: MediatorHttp;
  private readonly bindMethod: BindMethod;

  private readonly endpointFetcher: SparqlEndpointFetcher;
  private readonly cache: LRUCache<string, RDF.QueryResultCardinality> | undefined;

  private lastSourceContext: IActionContext | undefined;

  public constructor(
    url: string,
    context: IActionContext,
    mediatorHttp: MediatorHttp,
    bindMethod: BindMethod,
    forceHttpGet: boolean,
    cacheSize: number,
  ) {
    this.url = url;
    this.context = context;
    this.mediatorHttp = mediatorHttp;
    this.bindMethod = bindMethod;
    this.endpointFetcher = new SparqlEndpointFetcher({
      method: forceHttpGet ? 'GET' : 'POST',
      fetch: (input: Request | string, init?: RequestInit) => this.mediatorHttp.mediate(
        { input, init, context: this.lastSourceContext! },
      ),
      prefixVariableQuestionMark: true,
    });
    this.cache = cacheSize > 0 ?
      new LRUCache<string, RDF.QueryResultCardinality>({ max: cacheSize }) :
      undefined;
  }

  public async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public queryBindings(
    operationIn: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    // If bindings are passed, modify the operations
    let operationPromise: Promise<Algebra.Operation>;
    if (options?.joinBindings) {
      operationPromise = QuerySourceSparql.addBindingsToOperation(this.bindMethod, operationIn, options.joinBindings);
    } else {
      operationPromise = Promise.resolve(operationIn);
    }

    // Emit metadata containing the estimated count (reject is never called)
    let variablesCount: RDF.Variable[] = [];
    new Promise<RDF.QueryResultCardinality>(async resolve => {
      // Prepare queries
      const operation = await operationPromise;
      variablesCount = Util.inScopeVariables(operation);
      const countQuery: string = QuerySourceSparql.operationToCountQuery(operation);

      const cachedCardinality = this.cache?.get(countQuery);
      if (cachedCardinality !== undefined) {
        return resolve(cachedCardinality);
      }

      const bindingsStream: BindingsStream = this
        .queryBindingsRemote(this.url, countQuery, context, abortController.signal);
      bindingsStream.on('data', (bindings: Bindings) => {
        const count = bindings.get(VAR_COUNT);
        const cardinality: RDF.QueryResultCardinality = { type: 'estimate', value: Number.POSITIVE_INFINITY };
        if (count) {
          const cardinalityValue: number = Number.parseInt(count.value, 10);
          if (!Number.isNaN(cardinalityValue)) {
            cardinality.type = 'exact';
            cardinality.value = cardinalityValue;
            this.cache?.set(countQuery, cardinality);
          }
        }
        return resolve(cardinality);
      });
      bindingsStream.on('error', () => resolve({ type: 'estimate', value: Number.POSITIVE_INFINITY }));
      bindingsStream.on('end', () => resolve({ type: 'estimate', value: Number.POSITIVE_INFINITY }));
    })
      .then(cardinality => quads.setProperty('metadata', {
        cardinality,
        canContainUndefs: false,
        variables: variablesCount,
      }))
      .catch(() => quads.setProperty('metadata', {
        cardinality: { type: 'estimate', value: Number.POSITIVE_INFINITY },
        canContainUndefs: false,
        variables: variablesCount,
      }));

    const abortController = new AbortController();
    const quads: BindingsStream = new TransformIterator(async() => {
      // Prepare queries
      const operation = await operationPromise;
      const variables: RDF.Variable[] = Util.inScopeVariables(operation);
      const selectQuery: string = QuerySourceSparql.operationToSelectQuery(operation, variables);

      return this.queryBindingsRemote(this.url, selectQuery, context, abortController.signal);
    }, { autoStart: false });

    return quads;
  }

  public queryQuads(operation: Algebra.Construct, context: IActionContext): AsyncIterator<RDF.Quad> {
    this.lastSourceContext = context ? this.context.merge(context) : this.context;
    const rawStream = this.endpointFetcher.fetchTriples(this.url, QuerySourceSparql.operationToQuery(operation));
    this.lastSourceContext = undefined;
    return wrap<any>(rawStream, { autoStart: false, maxBufferSize: Number.POSITIVE_INFINITY });
  }

  public queryBoolean(operation: Algebra.Ask, context: IActionContext): Promise<boolean> {
    this.lastSourceContext = context ? this.context.merge(context) : this.context;
    const promise = this.endpointFetcher.fetchAsk(this.url, QuerySourceSparql.operationToQuery(operation));
    this.lastSourceContext = undefined;
    return promise;
  }

  public queryVoid(operation: Algebra.Update, context: IActionContext): Promise<void> {
    this.lastSourceContext = context ? this.context.merge(context) : this.context;
    const promise = this.endpointFetcher.fetchUpdate(this.url, QuerySourceSparql.operationToQuery(operation));
    this.lastSourceContext = undefined;
    return promise;
  }

  public static async addBindingsToOperation(
    bindMethod: BindMethod,
    operation: Algebra.Operation,
    addBindings: { bindings: BindingsStream; metadata: MetadataBindings },
  ): Promise<Algebra.Operation> {
    const bindings = await addBindings.bindings.toArray();

    switch (bindMethod) {
      case 'values':
        return AF.createJoin([
          AF.createValues(
            addBindings.metadata.variables,
            bindings.map(binding => Object.fromEntries([ ...binding ]
              .map(([ key, value ]) => [ `?${key.value}`, <RDF.Literal | RDF.NamedNode> value ]))),
          ),
          operation,
        ], false);
      case 'union': { throw new Error('Not implemented yet: "union" case'); } // TODO?
      case 'filter': { throw new Error('Not implemented yet: "filter" case'); } // TODO?
    }
  }

  /**
   * Convert an operation to a select query for this pattern.
   * @param {Algebra.Operation} operation A query operation.
   * @param {RDF.Variable[]} variables The variables in scope for the operation.
   * @return {string} A select query string.
   */
  public static operationToSelectQuery(operation: Algebra.Operation, variables: RDF.Variable[]): string {
    return QuerySourceSparql.operationToQuery(AF.createProject(operation, variables));
  }

  /**
   * Convert an operation to a count query for the number of matching triples for this pattern.
   * @param {Algebra.Operation} operation A query operation.
   * @return {string} A count query string.
   */
  public static operationToCountQuery(operation: Algebra.Operation): string {
    return QuerySourceSparql.operationToQuery(AF.createProject(
      AF.createExtend(
        AF.createGroup(
          operation,
          [],
          [ AF.createBoundAggregate(
            DF.variable('var0'),
            'count',
            AF.createWildcardExpression(),
            false,
          ) ],
        ),
        DF.variable('count'),
        AF.createTermExpression(DF.variable('var0')),
      ),
      [ DF.variable('count') ],
    ));
  }

  /**
   * Convert an operation to a query for this pattern.
   * @param {Algebra.Operation} operation A query operation.
   * @return {string} A query string.
   */
  public static operationToQuery(operation: Algebra.Operation): string {
    return toSparql(operation, { sparqlStar: true });
  }

  /**
   * Send a SPARQL query to a SPARQL endpoint and retrieve its bindings as a stream.
   * @param {string} endpoint A SPARQL endpoint URL.
   * @param {string} query A SPARQL query string.
   * @param {IActionContext} context The source context.
   * @param {AbortSignal} signal A signal for aborting the request.
   * @return {BindingsStream} A stream of bindings.
   */
  public queryBindingsRemote(
    endpoint: string,
    query: string,
    context: IActionContext | undefined,
    signal: AbortSignal | undefined,
  ): BindingsStream {
    this.lastSourceContext = context ? this.context.merge(context) : this.context;
    const rawStream = this.endpointFetcher.fetchBindings(endpoint, query);
    this.lastSourceContext = undefined;

    return wrap<any>(rawStream, { autoStart: false, maxBufferSize: Number.POSITIVE_INFINITY })
      .map((rawData: Record<string, RDF.Term>) => BF.bindings(Object.entries(rawData)
        .map(([ key, value ]) => [ DF.variable(key.slice(1)), value ])));
  }

  public toString(): string {
    return `QuerySourceSparql(${this.url})`;
  }
}
