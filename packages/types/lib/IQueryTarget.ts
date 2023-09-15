import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import type { Algebra } from 'sparqlalgebrajs';
import type { BindingsStream } from './Bindings';
import type { IActionContext } from './IActionContext';

// TODO: rm

export interface IQueryTargetSerialized extends IQueryTargetUnidentifiedExpanded {
  type?: 'stringSource';
  value: string;
  mediaType: string;
  baseIRI?: string;
}

export interface IQueryTargetUnidentifiedExpanded {
  type?: string;
  value: string | RDF.Source | RDF.Store;
  context?: IActionContext;
}

export type QueryTargetUnidentifiedExpanded = IQueryTargetUnidentifiedExpanded | IQueryTargetSerialized;
export type QueryTargetUnidentified = string | RDF.Source | RDF.Store | QueryTargetUnidentifiedExpanded;

export type QueryTarget = IQuerySource | IQueryDestination | (IQuerySource & IQueryDestination);

/**
 * Attaches a context to a query target.
 */
export interface IQueryTargetWrapper<Q extends QueryTarget = QueryTarget> {
  target: QueryTarget;
  context?: IActionContext;
}

export interface IQueryInterface<C> {
  getCapabilities: () => Promise<C>;
}

/**
 * A lazy query source.
 */
export interface IQuerySource extends IQueryInterface<{ read: boolean }> {
  /**
   * Get the selector type that is supported by this source.
   */
  getSelectorShape: () => Promise<FragmentSelectorShape>;

  /**
   * Returns a (possibly lazy) stream that returns all bindings matching the operation.
   *
   * Passed operations MUST conform to the query shape exposed by the selector type returned from `getSelectorShape`.
   * The given operation represents a Linked Data Fragments selector.
   *
   * The returned stream MUST expose the property 'metadata' of type `MetadataBindings`.
   * The implementor is reponsible for handling cases where 'metadata'
   * is being called without the stream being in flow-mode.
   * This metadata object can become invalidated (see `metadata.state`),
   * in which case the 'metadata' property must and will be updated.
   *
   * @param {Algebra.Operation} operation The query operation to execute.
   * @param {IActionContext} context      The query context.
   * @return {AsyncIterator<RDF.Quad>} The resulting bindings stream.
   *
   * @see https://linkeddatafragments.org/specification/linked-data-fragments/#selectors
   */
  executeQuery: (
    operation: Algebra.Operation,
    context: IActionContext,
  ) => Promise<BindingsStream>;
}

/**
 * A lazy query source.
 */
export interface IQueryDestination extends IQueryInterface<{ write: boolean }> {
  /**
   * Insert the given quad stream into the destination.
   * @param quads The quads to insert.
   * @return {AsyncIterator<RDF.Quad>} The inserted quad stream.
   */
  insert: (quads: AsyncIterator<RDF.Quad>) => Promise<void>;
  /**
   * Delete the given quad stream from the destination.
   * @param quads The quads to delete.
   * @return {AsyncIterator<RDF.Quad>} The deleted quad stream.
   */
  delete: (quads: AsyncIterator<RDF.Quad>) => Promise<void>;
  /**
   * Graphs that should be deleted.
   * @param graphs The graph(s) in which all triples must be removed.
   * @param requireExistence If true, and any of the graphs does not exist, an error must be emitted.
   *                         Should only be considered on destinations that record empty graphs.
   * @param dropGraphs If the graphs themselves should also be dropped.
   *                   Should not happen on the 'DEFAULT' graph.
   *                   Should only be considered on destinations that record empty graphs.
   */
  deleteGraphs: (
    graphs: RDF.DefaultGraph | 'NAMED' | 'ALL' | RDF.NamedNode[],
    requireExistence: boolean,
    dropGraphs: boolean,
  ) => Promise<void>;
  /**
   * Create the given (empty) graphs.
   * @param graphs The graph names to create.
   * @param requireNonExistence If true, an error MUST be thrown when any of the graph already exists.
   *                            For destinations that do not record empty graphs,
   *                            this should only throw if at least one quad with the given quad already exists.
   */
  createGraphs: (graphs: RDF.NamedNode[], requireNonExistence: boolean) => Promise<void>;
}

/**
 * A fragment selector shape determines the shape of selectors that can be executed by a query source.
 * Selectors conforming to this shape represent boolean functions to decide if triples belong to a query response.
 * @see https://linkeddatafragments.org/specification/linked-data-fragments/#selectors
 */
type FragmentSelectorShape = {
  type: 'operation';
  /**
   * The supported operation.
   */
  operation: {
    type: Algebra.types;
  } | {
    pattern: Algebra.Operation;
  };
  /**
   * Variables that are in-scope in this operation and its children.
   */
  scopedVariables?: RDF.Variable[];
  /**
   * Variables that must be passed to the selector when instantiated.
   */
  variablesRequired?: RDF.Variable[];
  /**
   * Variables that may be passed to the selector when instantiated.
   */
  variablesOptional?: RDF.Variable[];
  /**
   * Children of this operation.
   */
  children?: FragmentSelectorShape[];
  /**
   * If bindings can be passed into the source.
   */
  addBindings?: true;
} | {
  type: 'conjunction';
  children: FragmentSelectorShape[];
} | {
  type: 'disjunction';
  children: FragmentSelectorShape[];
} | {
  type: 'arity';
  min?: number;
  max?: number;
  child: FragmentSelectorShape;
};

// ----- Examples of FragmentSelectorShapes -----
// const AF = new Factory();
// const DF = new DataFactory();
// const shapeTpf: FragmentSelectorShape = {
//   type: 'operation',
//   operation: { pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o')) },
//   variablesOptional: [
//     DF.variable('s'),
//     DF.variable('p'),
//     DF.variable('o'),
//   ],
// };
//
// const shapeQpf: FragmentSelectorShape = {
//   type: 'operation',
//   operation: { pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o'), DF.variable('g')) },
//   variablesOptional: [
//     DF.variable('s'),
//     DF.variable('p'),
//     DF.variable('o'),
//     DF.variable('g'),
//   ],
// };
//
// const shapeBrTpf: FragmentSelectorShape = {
//   type: 'operation',
//   operation: { pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o')) },
//   variablesOptional: [
//     DF.variable('s'),
//     DF.variable('p'),
//     DF.variable('o'),
//   ],
//   addBindings: true,
// };
//
// const shapeSparqlEp: FragmentSelectorShape = { // Same as SaGe
//   type: 'disjunction',
//   children: [
//     {
//       type: 'operation',
//       operation: { type: Algebra.types.PROJECT },
//     },
//     {
//       type: 'operation',
//       operation: { type: Algebra.types.CONSTRUCT },
//     },
//     {
//       type: 'operation',
//       operation: { type: Algebra.types.DESCRIBE },
//     },
//     {
//       type: 'operation',
//       operation: { type: Algebra.types.ASK },
//     },
//     {
//       type: 'operation',
//       operation: { type: Algebra.types.COMPOSITE_UPDATE },
//     },
//   ],
// };
//
// // Example of request:
// //   Find ?s matching "?s dbo:country dbr:norway. ?s dbo:award ?o2. ?s dbo:birthDate ?o3."
// const shapeSpf: FragmentSelectorShape = {
//   type: 'operation',
//   operation: { type: Algebra.types.BGP },
//   scopedVariables: [
//     DF.variable('s'),
//   ],
//   children: [
//     {
//       type: 'arity',
//       min: 1,
//       max: Number.POSITIVE_INFINITY,
//       child: {
//         type: 'operation',
//         operation: { pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o')) },
//         variablesOptional: [
//           DF.variable('p'),
//           DF.variable('o'),
//         ],
//       },
//     },
//   ],
//   addBindings: true,
// };
//
// // Example of requests:
// //   - brTPF
// //   - Find all ?s and ?o matching "?s db:country ?o"
// const shapeSmartKg: FragmentSelectorShape = {
//   type: 'disjunction',
//   children: [
//     {
//       type: 'operation',
//       operation: { pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o')) },
//       variablesOptional: [
//         DF.variable('s'),
//         DF.variable('p'),
//         DF.variable('o'),
//       ],
//       addBindings: true,
//     },
//     {
//       type: 'operation',
//       operation: { type: Algebra.types.BGP },
//       children: [
//         {
//           type: 'arity',
//           min: 1,
//           max: Number.POSITIVE_INFINITY,
//           child: {
//             type: 'operation',
//             operation: { pattern: AF.createPattern(DF.variable('s'), DF.variable('p'), DF.variable('o')) },
//             variablesRequired: [
//               DF.variable('p'),
//             ],
//           },
//         },
//       ],
//     },
//   ],
// };
