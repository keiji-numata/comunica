import type { ISearchForm } from '@comunica/actor-rdf-metadata-extract-hydra-controls';
import { bindingsToString } from '@comunica/bindings-factory';
import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import { quadsToBindings } from '@comunica/bus-query-source-identify';
import type { MediatorRdfMetadata, IActorRdfMetadataOutput } from '@comunica/bus-rdf-metadata';
import type { MediatorRdfMetadataExtract } from '@comunica/bus-rdf-metadata-extract';
import { KeysQueryOperation } from '@comunica/context-entries';
import type { IQuerySource, BindingsStream, IActionContext, FragmentSelectorShape, IQueryBindingsOptions, MetadataBindings } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { ArrayIterator, TransformIterator, wrap } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { termToString } from 'rdf-string';
import { termToString as termToStringTtl } from 'rdf-string-ttl';
import {
  everyTermsNested,
  mapTerms,
  matchPattern,
  matchPatternComplete,
  someTerms,
} from 'rdf-terms';
import type { Algebra } from 'sparqlalgebrajs';
import { Factory } from 'sparqlalgebrajs';

const AF = new Factory();
const DF = new DataFactory<RDF.BaseQuad>();

export class QuerySourceQpf implements IQuerySource {
  protected readonly selectorShape: FragmentSelectorShape = {
    type: 'operation',
    operation: {
      operationType: 'pattern',
      pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o'), DF.variable('g')),
    },
    variablesOptional: [
      DF.variable('s'),
      DF.variable('p'),
      DF.variable('o'),
      DF.variable('g'),
    ],
  };

  protected readonly selectorShapeBindingsRestricted: FragmentSelectorShape = {
    type: 'operation',
    operation: {
      operationType: 'pattern',
      pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o'), DF.variable('g')),
    },
    variablesOptional: [
      DF.variable('s'),
      DF.variable('p'),
      DF.variable('o'),
      DF.variable('g'),
    ],
    filterBindings: true,
  };

  public readonly searchForm: ISearchForm;

  private readonly mediatorMetadata: MediatorRdfMetadata;

  private readonly mediatorMetadataExtract: MediatorRdfMetadataExtract;

  private readonly mediatorDereferenceRdf: MediatorDereferenceRdf;

  private readonly subjectUri: string;
  private readonly predicateUri: string;
  private readonly objectUri: string;
  private readonly graphUri?: string;
  private readonly bindingsRestrictedChunkSize: number;
  private readonly url: string;
  private readonly defaultGraph?: RDF.NamedNode;
  private readonly context: IActionContext;
  private readonly bindingsRestricted: boolean;
  private readonly cachedQuads: Record<string, AsyncIterator<RDF.Quad>>;

  public constructor(
    mediatorMetadata: MediatorRdfMetadata,
    mediatorMetadataExtract: MediatorRdfMetadataExtract,
    mediatorDereferenceRdf: MediatorDereferenceRdf,
    subjectUri: string, predicateUri: string, objectUri: string, graphUri: string | undefined,
    bindingsRestrictedChunkSize: number,
    url: string,
    metadata: Record<string, any>, context: IActionContext, bindingsRestricted: boolean,
    initialQuads?: RDF.Stream,
  ) {
    this.mediatorMetadata = mediatorMetadata;
    this.mediatorMetadataExtract = mediatorMetadataExtract;
    this.mediatorDereferenceRdf = mediatorDereferenceRdf;
    this.subjectUri = subjectUri;
    this.predicateUri = predicateUri;
    this.objectUri = objectUri;
    this.graphUri = graphUri;
    this.bindingsRestrictedChunkSize = bindingsRestrictedChunkSize;
    this.url = url;
    this.context = context;
    this.bindingsRestricted = bindingsRestricted;
    this.cachedQuads = {};
    const searchForm = this.getSearchForm(metadata);
    if (!searchForm) {
      throw new Error('Illegal state: found no TPF/QPF search form anymore in metadata.');
    }
    this.searchForm = searchForm;
    this.defaultGraph = metadata.defaultGraph ? DF.namedNode(metadata.defaultGraph) : undefined;
    if (initialQuads) {
      let wrappedQuads: AsyncIterator<RDF.Quad> = wrap<RDF.Quad>(initialQuads);
      if (this.defaultGraph) {
        wrappedQuads = this.reverseMapQuadsToDefaultGraph(wrappedQuads);
      }
      wrappedQuads.setProperty('metadata', metadata);
      this.cacheQuads(wrappedQuads, DF.variable(''), DF.variable(''), DF.variable(''), DF.variable(''));
    }
  }

  public async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.bindingsRestricted ? this.selectorShapeBindingsRestricted : this.selectorShape;
  }

  public queryBindings(
    operation: Algebra.Operation,
    context: IActionContext,
    options?: IQueryBindingsOptions,
  ): BindingsStream {
    if (operation.type !== 'pattern') {
      throw new Error(`Attempted to pass non-pattern operation '${operation.type}' to QuerySourceQpf`);
    }

    // Create an async iterator from the matched quad stream
    // TODO: handle reused variables
    let it = this.match(
      // TODO: nullify vars?
      operation.subject,
      operation.predicate,
      operation.object,
      operation.graph,
      options,
    );

    // Perform post-match-filtering if the source does not support quoted triple filtering,
    // but we have a variable inside a quoted triple.
    // TODO: abstract all this stuff, as it's the same as in QuerySourceRdfJs
    if (someTerms(operation, term => term.termType === 'Quad')) {
      it = it.filter(quad => matchPatternComplete(quad, operation));
    }

    return quadsToBindings(it, operation, Boolean(context.get(KeysQueryOperation.unionDefaultGraph)));
  }

  /**
   * Get a first QPF search form.
   * @param {{[p: string]: any}} metadata A metadata object.
   * @return {ISearchForm} A search form, or null if none could be found.
   */
  public getSearchForm(metadata: Record<string, any>): ISearchForm | undefined {
    if (!metadata.searchForms || !metadata.searchForms.values) {
      return;
    }

    // Find a quad pattern or triple pattern search form
    const { searchForms } = metadata;
    for (const searchForm of searchForms.values) {
      if (this.graphUri &&
        this.subjectUri in searchForm.mappings &&
        this.predicateUri in searchForm.mappings &&
        this.objectUri in searchForm.mappings &&
        this.graphUri in searchForm.mappings &&
        Object.keys(searchForm.mappings).length === 4) {
        return searchForm;
      }
      if (this.subjectUri in searchForm.mappings &&
        this.predicateUri in searchForm.mappings &&
        this.objectUri in searchForm.mappings &&
        Object.keys(searchForm.mappings).length === 3) {
        return searchForm;
      }
    }
  }

  /**
   * Create a QPF fragment IRI for the given quad pattern.
   * @param {ISearchForm} searchForm A search form.
   * @param {Term} subject A term.
   * @param {Term} predicate A term.
   * @param {Term} object A term.
   * @param {Term} graph A term.
   * @return {string} A URI.
   */
  public createFragmentUri(searchForm: ISearchForm,
    subject: RDF.Term, predicate: RDF.Term, object: RDF.Term, graph: RDF.Term): string {
    const entries: Record<string, string> = {};
    const input = [
      { uri: this.subjectUri, term: subject },
      { uri: this.predicateUri, term: predicate },
      { uri: this.objectUri, term: object },
      { uri: this.graphUri, term: graph },
    ];
    for (const entry of input) {
      // If bindingsRestricted, also pass variables, so the server knows how to bind values.
      if (entry.uri && (this.bindingsRestricted || (entry.term.termType !== 'Variable' &&
        (entry.term.termType !== 'Quad' || everyTermsNested(entry.term, value => value.termType !== 'Variable'))))) {
        entries[entry.uri] = termToString(entry.term);
      }
    }
    return searchForm.getUri(entries);
  }

  public match(
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
    graph: RDF.Term,
    options?: IQueryBindingsOptions,
  ): AsyncIterator<RDF.Quad> {
    // If we are querying the default graph,
    // and the source has an overridden value for the default graph (such as QPF can provide),
    // we override the graph parameter with that value.
    let modifiedGraph = false;
    if (graph.termType === 'DefaultGraph') {
      if (this.defaultGraph) {
        modifiedGraph = true;
        graph = this.defaultGraph;
      } else if (Object.keys(this.searchForm.mappings).length === 4 && !this.defaultGraph) {
        // If the sd:defaultGraph is not declared on a QPF endpoint,
        // then the default graph must be empty.
        const quads = new ArrayIterator([], { autoStart: false });
        quads.setProperty('metadata', {
          requestTime: 0,
          cardinality: { type: 'exact', value: 0 },
          first: null,
          next: null,
          last: null,
          canContainUndefs: false,
        });
        return quads;
      }
    }

    // Try to emit from cache (skip if filtering bindings)
    if (!options?.filterBindings) {
      const cached = this.getCachedQuads(subject, predicate, object, graph);
      if (cached) {
        return cached;
      }
    }

    // Kickstart metadata collection, because the quads iterator is lazy
    const rdfMetadataOuputPromise = new Promise<IActorRdfMetadataOutput>(async resolve => {
      let url: string = this.createFragmentUri(this.searchForm, subject, predicate, object, graph);

      // Handle bindings-restricted interfaces
      let nextUrls: string[] = [];
      if (options?.filterBindings) {
        nextUrls = await this.getBindingsRestrictedLinks(
          subject,
          predicate,
          object,
          graph,
          url,
          options.filterBindings,
        );
        if (nextUrls.length > 0) {
          url = nextUrls.splice(0, 1)[0];
        }
      }

      const dereferenceRdfOutput = await this.mediatorDereferenceRdf.mediate({ context: this.context, url });
      url = dereferenceRdfOutput.url;

      // Determine the metadata
      const rdfMetadataOuput: IActorRdfMetadataOutput = await this.mediatorMetadata.mediate(
        { context: this.context,
          url,
          quads: dereferenceRdfOutput.data,
          triples: dereferenceRdfOutput.metadata?.triples },
      );

      // Extract the metadata
      const { metadata } = await this.mediatorMetadataExtract
        .mediate({
          context: this.context,
          url,
          metadata: rdfMetadataOuput.metadata,
          requestTime: dereferenceRdfOutput.requestTime,
        });
      // Forcefully add next urls from brTpf to metadata if needed
      if (nextUrls.length > 0) {
        if (!metadata.next) {
          metadata.next = [];
        }
        metadata.next = [ ...metadata.next, ...nextUrls ];
      }
      quads.setProperty('metadata', { ...metadata, canContainUndefs: false, subsetOf: this.url });

      // While we could resolve this before metadata extraction, we do it afterwards to ensure metadata emission
      // before the end event is emitted.
      resolve(rdfMetadataOuput);
    });

    const quads = new TransformIterator(async() => {
      const rdfMetadataOuput = await rdfMetadataOuputPromise;

      // The server is free to send any data in its response (such as metadata),
      // including quads that do not match the given matter.
      // Therefore, we have to filter away all non-matching quads here.
      const actualDefaultGraph = DF.defaultGraph();
      let filteredOutput: AsyncIterator<RDF.Quad> = wrap<RDF.Quad>(rdfMetadataOuput.data)
        .transform({
          filter(quad: RDF.Quad) {
            if (matchPattern(quad, subject, predicate, object, graph)) {
              return true;
            }
            // Special case: if we are querying in the default graph, and we had an overridden default graph,
            // also accept that incoming triples may be defined in the actual default graph
            return modifiedGraph && matchPattern(quad, subject, predicate, object, actualDefaultGraph);
          },
        });
      if (modifiedGraph || graph.termType === 'Variable') {
        // Reverse-map the overridden default graph back to the actual default graph
        filteredOutput = this.reverseMapQuadsToDefaultGraph(filteredOutput);
      }

      // Swallow error events, as they will be emitted in the metadata stream as well,
      // and therefore thrown async next.
      filteredOutput.on('error', () => {
        // Do nothing
      });

      return filteredOutput;
    }, { autoStart: false });

    // Skip cache if filtering bindings
    if (options?.filterBindings) {
      return quads;
    }

    this.cacheQuads(quads, subject, predicate, object, graph);
    return this.getCachedQuads(subject, predicate, object, graph)!;
  }

  /**
   * If we add bindings for brTPF, append it to the URL.
   * We have to hardcode this because brTPF doesn't expose a URL template for passing bindings.
   * @param subject The subject.
   * @param predicate The predicate.
   * @param object The object.
   * @param graph The graph.
   * @param url The original QPF URL.
   * @param filterBindings The bindings to restrict with.
   * @protected
   */
  protected async getBindingsRestrictedLinks(
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
    graph: RDF.Term,
    url: string,
    filterBindings: { bindings: BindingsStream; metadata: MetadataBindings },
  ): Promise<string[]> {
    const nextUrls: string[] = [];

    // Determine common variables
    const commonVariables: RDF.Variable[] = [];
    for (const variable of filterBindings.metadata.variables) {
      if (subject.termType === 'Variable' && subject.equals(variable)) {
        commonVariables.push(subject);
      }
      if (predicate.termType === 'Variable' && predicate.equals(variable)) {
        commonVariables.push(predicate);
      }
      if (object.termType === 'Variable' && object.equals(variable)) {
        commonVariables.push(object);
      }
      if (graph.termType === 'Variable' && graph.equals(variable)) {
        commonVariables.push(graph);
      }
    }

    // Only pass bindings if the variables of the bindings overlap with the pattern
    if (commonVariables.length > 0) {
      // Determine values
      let bindings = await filterBindings.bindings.toArray();

      // Make bindings unique, and only include the common variables in the bindings
      const hashes: Record<string, boolean> = {};
      bindings = bindings
        .map(binding => binding
          .filter((value, key) => commonVariables.some(commonVariable => commonVariable.equals(key))))
        .filter(binding => {
          const hash: string = bindingsToString(binding);
          // eslint-disable-next-line no-return-assign
          return !(hash in hashes) && (hashes[hash] = true);
        });

      while (bindings.length > 0) {
        const values: string[] = [];
        for (const binding of bindings.splice(0, this.bindingsRestrictedChunkSize)) {
          const value: string[] = [ '(' ];
          for (const variable of commonVariables) {
            const term = binding.get(variable);
            value.push(term ? termToStringTtl(term) : 'UNDEF');
            value.push(' ');
          }
          value.push(')');
          values.push(value.join(''));
        }

        if (values.length === 0) {
          // This is a hack to force an empty result page,
          // because the brTPF server returns a server error when passing 0 bindings.
          values.push('(<ex:comunica:unknown>)');
        }

        // Append to URL (brTPF uses the SPARQL VALUES syntax, without the VALUES prefix)
        const valuesUrl = encodeURIComponent(`(${commonVariables.map(variable => `?${variable.value}`).join(' ')}) { ${values.join(' ')} }`);
        nextUrls.push(`${url}&values=${valuesUrl}`);
      }
    }

    return nextUrls;
  }

  protected reverseMapQuadsToDefaultGraph(quads: AsyncIterator<RDF.Quad>): AsyncIterator<RDF.Quad> {
    const actualDefaultGraph = DF.defaultGraph();
    return quads.map(
      quad => mapTerms(quad,
        (term, key) => key === 'graph' && term.equals(this.defaultGraph) ? actualDefaultGraph : term),
    );
  }

  protected getPatternId(subject: RDF.Term, predicate: RDF.Term, object: RDF.Term, graph: RDF.Term): string {
    /* eslint-disable id-length */
    return JSON.stringify({
      s: subject.termType === 'Variable' ? '' : _termToString(subject),
      p: predicate.termType === 'Variable' ? '' : _termToString(predicate),
      o: object.termType === 'Variable' ? '' : _termToString(object),
      g: graph.termType === 'Variable' ? '' : _termToString(graph),
    });
    /* eslint-enable id-length */
  }

  protected cacheQuads(quads: AsyncIterator<RDF.Quad>,
    subject: RDF.Term, predicate: RDF.Term, object: RDF.Term, graph: RDF.Term): void {
    const patternId = this.getPatternId(subject, predicate, object, graph);
    this.cachedQuads[patternId] = quads.clone();
  }

  protected getCachedQuads(subject: RDF.Term, predicate: RDF.Term, object: RDF.Term, graph: RDF.Term):
  AsyncIterator<RDF.Quad> | undefined {
    const patternId = this.getPatternId(subject, predicate, object, graph);
    const quads = this.cachedQuads[patternId];
    if (quads) {
      return quads.clone();
    }
  }

  public queryQuads(operation: Algebra.Construct, context: IActionContext): AsyncIterator<RDF.Quad> {
    throw new Error('queryQuads is not implemented in QuerySourceQpf');
  }

  public queryBoolean(operation: Algebra.Ask, context: IActionContext): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in QuerySourceQpf');
  }

  public queryVoid(operation: Algebra.Update, context: IActionContext): Promise<void> {
    throw new Error('queryVoid is not implemented in QuerySourceQpf');
  }
}

function _termToString(term: RDF.Term): string {
  return term.termType === 'DefaultGraph' ?
    // Any character that cannot be present in a URL will do
    '|' :
    termToString(term);
}
