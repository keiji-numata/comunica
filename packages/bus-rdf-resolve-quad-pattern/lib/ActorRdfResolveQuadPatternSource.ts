import type { IActorTest } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import type { Algebra } from 'sparqlalgebrajs';
import type { IActionRdfResolveQuadPattern,
  IActorRdfResolveQuadPatternOutput, IActorRdfResolveQuadPatternArgs } from './ActorRdfResolveQuadPattern';
import { ActorRdfResolveQuadPattern } from './ActorRdfResolveQuadPattern';

/**
 * A base implementation for rdf-resolve-quad-pattern events
 * that wraps around an {@link IQuadSource}.
 *
 * @see IQuadSource
 */
export abstract class ActorRdfResolveQuadPatternSource extends ActorRdfResolveQuadPattern {
  public constructor(args: IActorRdfResolveQuadPatternArgs) {
    super(args);
  }

  public async test(action: IActionRdfResolveQuadPattern): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionRdfResolveQuadPattern): Promise<IActorRdfResolveQuadPatternOutput> {
    const source = await this.getSource(action.context, action.pattern);
    return await this.getOutput(source, action.pattern, action.context);
  }

  /**
   * Get the output of the given action on a source.
   * @param {IQuadSource} source A quad source, possibly lazy.
   * @param {Algebra.Operation} pattern The operation to apply.
   * @param {IActionContext} context Optional context data.
   * @return {Promise<IActorRdfResolveQuadPatternOutput>} A promise that resolves to a hash containing
   *                                                      a data RDFJS stream.
   */
  protected async getOutput(source: IQuadSource, pattern: RDF.BaseQuad, context: IActionContext):
  Promise<IActorRdfResolveQuadPatternOutput> {
    // Create data stream
    const data = source.match(pattern.subject, pattern.predicate, pattern.object, pattern.graph, context);
    return { data };
  }

  /**
   * Get a source instance for the given context.
   * @param {ActionContext} context Optional context data.
   * @param {Algebra.Pattern} operation The operation to apply.
   * @return {Promise<RDF.Source>} A promise that resolves to a source.
   */
  protected abstract getSource(context: IActionContext, operation: Algebra.Pattern): Promise<IQuadSource>;
}

/**
 * A lazy quad source.
 */
export interface IQuadSource {
  /**
   * Returns a (possibly lazy) stream that processes all quads matching the pattern.
   *
   * The returned stream MUST expose the property 'metadata' of type `MetadataQuads`.
   * The implementor is reponsible for handling cases where 'metadata'
   * is being called without the stream being in flow-mode.
   * This metadata object can become invalidated (see `metadata.state`),
   * in which case the 'metadata' property must and will be updated.
   *
   * @param {RDF.Term} subject   The exact subject to match, variable is wildcard.
   * @param {RDF.Term} predicate The exact predicate to match, variable is wildcard.
   * @param {RDF.Term} object    The exact object to match, variable is wildcard.
   * @param {RDF.Term} graph     The exact graph to match, variable is wildcard.
   * @param {IActionContext} context The query context.
   * @return {AsyncIterator<RDF.Quad>} The resulting quad stream.
   */
  match: (
    subject: RDF.Term,
    predicate: RDF.Term,
    object: RDF.Term,
    graph: RDF.Term,
    // TODO: in next major update, make this mandatory.
    context?: IActionContext,
  ) => AsyncIterator<RDF.Quad>;
}
