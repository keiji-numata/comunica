import { LinkQueueFifo } from '@comunica/actor-rdf-resolve-hypermedia-links-queue-fifo';
import { BindingsFactory } from '@comunica/bindings-factory';
import type { MediatorRdfMetadataAccumulate } from '@comunica/bus-rdf-metadata-accumulate';
import type { MediatorRdfResolveHypermediaLinks, ILink } from '@comunica/bus-rdf-resolve-hypermedia-links';
import type { MediatorRdfResolveHypermediaLinksQueue } from '@comunica/bus-rdf-resolve-hypermedia-links-queue';
import { ActionContext } from '@comunica/core';
import type { IActionContext, IQuerySource } from '@comunica/types';
import { DataFactory } from 'rdf-data-factory';
import type { Algebra } from 'sparqlalgebrajs';
import { Factory } from 'sparqlalgebrajs';
import type { ISourceState, SourceStateGetter } from '../lib/LinkedRdfSourcesAsyncRdfIterator';
import { MediatedLinkedRdfSourcesAsyncRdfIterator } from '../lib/MediatedLinkedRdfSourcesAsyncRdfIterator';

const DF = new DataFactory();
const AF = new Factory();
const BF = new BindingsFactory();

describe('MediatedLinkedRdfSourcesAsyncRdfIterator', () => {
  describe('A MediatedLinkedRdfSourcesAsyncRdfIterator instance', () => {
    let context: IActionContext;
    let source: any;
    let operation: Algebra.Operation;
    let mediatorMetadataAccumulate: MediatorRdfMetadataAccumulate;
    let mediatorRdfResolveHypermediaLinks: MediatorRdfResolveHypermediaLinks;
    let mediatorRdfResolveHypermediaLinksQueue: MediatorRdfResolveHypermediaLinksQueue;
    let sourceStateGetter: SourceStateGetter;

    beforeEach(() => {
      context = new ActionContext({});
      AF.createPattern(
        DF.namedNode('s'),
        DF.namedNode('p'),
        DF.namedNode('o'),
        DF.namedNode('g'),
      );
      mediatorRdfResolveHypermediaLinks = <any>{
        mediate: jest.fn(({ metadata }: any) => Promise
          .resolve({ links: [{ url: `${metadata.baseURL}url1` }, { url: `${metadata.baseURL}url2` }]})),
      };
      mediatorRdfResolveHypermediaLinksQueue = <any>{
        mediate: () => Promise.resolve({ linkQueue: new LinkQueueFifo() }),
      };
      sourceStateGetter = async(link: ILink, handledDatasets: Record<string, boolean>) => {
        return <ISourceState> <any> {
          link,
          metadata: {},
          handledDatasets: { ...handledDatasets, MYDATASET: true },
          source: <IQuerySource> {},
        };
      };
      source = new MediatedLinkedRdfSourcesAsyncRdfIterator(
        10,
        operation,
        {},
        context,
        'forcedType',
        'first',
        64,
        sourceStateGetter,
        undefined,
        mediatorMetadataAccumulate,
        mediatorRdfResolveHypermediaLinks,
        mediatorRdfResolveHypermediaLinksQueue,
      );
    });

    describe('close', () => {
      it('should not end an undefined aggregated store', async() => {
        source.close();
      });

      it('should end a defined aggregated store', async() => {
        const aggregatedStore: any = {
          end: jest.fn(),
          setBaseMetadata: jest.fn(),
          import: jest.fn(),
        };
        source = new MediatedLinkedRdfSourcesAsyncRdfIterator(
          10,
          operation,
          {},
          context,
          'forcedType',
          'first',
          64,
          sourceStateGetter,
          aggregatedStore,
          mediatorMetadataAccumulate,
          mediatorRdfResolveHypermediaLinks,
          mediatorRdfResolveHypermediaLinksQueue,
        );

        source.close();
        await new Promise(setImmediate);
        expect(aggregatedStore.end).toHaveBeenCalledTimes(1);
      });

      it('should close if the iterator is closeable', async() => {
        source.close();
        await new Promise(setImmediate);
        expect(source.closed).toEqual(true);
        expect(source.wasForcefullyClosed).toEqual(false);
      });

      it('should close if the iterator is closeable, and end the aggregated store', async() => {
        const aggregatedStore: any = {
          end: jest.fn(),
          setBaseMetadata: jest.fn(),
          import: jest.fn(),
        };
        source = new MediatedLinkedRdfSourcesAsyncRdfIterator(
          10,
          operation,
          {},
          context,
          'forcedType',
          'first',
          64,
          sourceStateGetter,
          aggregatedStore,
          mediatorMetadataAccumulate,
          mediatorRdfResolveHypermediaLinks,
          mediatorRdfResolveHypermediaLinksQueue,
        );

        source.close();
        await new Promise(setImmediate);
        expect(source.closed).toEqual(true);
        expect(aggregatedStore.end).toHaveBeenCalled();
        expect(source.wasForcefullyClosed).toEqual(false);
      });

      it('should not close if the iterator is not closeable', async() => {
        source.getLinkQueue = async() => ({ isEmpty: () => false });
        source.close();
        await new Promise(setImmediate);
        expect(source.closed).toEqual(false);
        expect(source.wasForcefullyClosed).toEqual(true);
      });

      it('should destroy if the link queue rejects', async() => {
        source.getLinkQueue = () => Promise.reject(new Error('getLinkQueue reject'));
        source.close();
        await expect(new Promise((resolve, reject) => source.on('error', reject)))
          .rejects.toThrow('getLinkQueue reject');
      });
    });

    describe('destroy', () => {
      it('should not end an undefined aggregated store', async() => {
        source.destroy();
      });

      it('should end a defined aggregated store', async() => {
        const aggregatedStore: any = {
          end: jest.fn(),
          setBaseMetadata: jest.fn(),
          import: jest.fn(),
        };
        source = new MediatedLinkedRdfSourcesAsyncRdfIterator(
          10,
          operation,
          {},
          context,
          'forcedType',
          'first',
          64,
          sourceStateGetter,
          aggregatedStore,
          mediatorMetadataAccumulate,
          mediatorRdfResolveHypermediaLinks,
          mediatorRdfResolveHypermediaLinksQueue,
        );

        source.destroy();
        await new Promise(setImmediate);
        expect(aggregatedStore.end).toHaveBeenCalledTimes(1);
      });

      it('should close if the iterator is closeable', async() => {
        source.destroy();
        await new Promise(setImmediate);
        expect(source.closed).toEqual(true);
        expect(source.wasForcefullyClosed).toEqual(false);
      });

      it('should close if the iterator is closeable, and end the aggregated store', async() => {
        const aggregatedStore: any = {
          end: jest.fn(),
          setBaseMetadata: jest.fn(),
          import: jest.fn(),
        };
        source = new MediatedLinkedRdfSourcesAsyncRdfIterator(
          10,
          operation,
          {},
          context,
          'forcedType',
          'first',
          64,
          sourceStateGetter,
          aggregatedStore,
          mediatorMetadataAccumulate,
          mediatorRdfResolveHypermediaLinks,
          mediatorRdfResolveHypermediaLinksQueue,
        );

        source.destroy();
        await new Promise(setImmediate);
        expect(source.closed).toEqual(true);
        expect(aggregatedStore.end).toHaveBeenCalled();
        expect(source.wasForcefullyClosed).toEqual(false);
      });

      it('should not close if the iterator is not closeable', async() => {
        source.getLinkQueue = async() => ({ isEmpty: () => false });
        source.destroy();
        await new Promise(setImmediate);
        expect(source.closed).toEqual(false);
        expect(source.wasForcefullyClosed).toEqual(true);
      });

      it('should destroy if the link queue rejects', async() => {
        source.getLinkQueue = () => Promise.reject(new Error('getLinkQueue reject'));
        source.destroy();
        await expect(new Promise((resolve, reject) => source.on('error', reject)))
          .rejects.toThrow('getLinkQueue reject');
      });
    });

    describe('getLinkQueue', () => {
      it('should return a new link queue when called for the first time', async() => {
        expect(await source.getLinkQueue()).toBeInstanceOf(LinkQueueFifo);
      });

      it('should always return the same link queue', async() => {
        const queue = await source.getLinkQueue();
        expect(await source.getLinkQueue()).toBe(queue);
        expect(await source.getLinkQueue()).toBe(queue);
        expect(await source.getLinkQueue()).toBe(queue);
        source.destroy();
      });

      it('should throw on a rejecting mediator', async() => {
        mediatorRdfResolveHypermediaLinksQueue.mediate = () => Promise
          .reject(new Error('mediatorRdfResolveHypermediaLinksQueue-error'));
        await expect(source.getLinkQueue()).rejects.toThrowError('mediatorRdfResolveHypermediaLinksQueue-error');
      });
    });

    describe('getSourceLinks', () => {
      it('should get urls based on mediatorRdfResolveHypermediaLinks', async() => {
        jest.spyOn(mediatorRdfResolveHypermediaLinks, 'mediate');
        expect(await source.getSourceLinks({ baseURL: 'http://base.org/' })).toEqual([
          { url: 'http://base.org/url1' },
          { url: 'http://base.org/url2' },
        ]);
        expect(mediatorRdfResolveHypermediaLinks.mediate)
          .toHaveBeenCalledWith({ context, metadata: { baseURL: 'http://base.org/' }});
      });

      it('should not emit any urls that were already emitted', async() => {
        source.handledUrls['http://base.org/url1'] = true;
        expect(await source.getSourceLinks({ baseURL: 'http://base.org/' })).toEqual([
          { url: 'http://base.org/url2' },
        ]);
      });

      it('should not re-emit any the first url', async() => {
        mediatorRdfResolveHypermediaLinks.mediate = () => Promise.resolve({ links: [{ url: 'first' }]});
        expect(await source.getSourceLinks({ baseURL: 'http://base.org/' })).toEqual([]);
      });

      it('should be invokable multiple times', async() => {
        expect(await source.getSourceLinks({ baseURL: 'http://base.org/' })).toEqual([
          { url: 'http://base.org/url1' },
          { url: 'http://base.org/url2' },
        ]);
        expect(await source.getSourceLinks({ baseURL: 'http://base2.org/' })).toEqual([
          { url: 'http://base2.org/url1' },
          { url: 'http://base2.org/url2' },
        ]);
        expect(await source.getSourceLinks({ baseURL: 'http://base.org/' })).toEqual([]); // Already handled
      });

      it('should return no urls on a rejecting mediator', async() => {
        mediatorRdfResolveHypermediaLinks.mediate = () => Promise.reject(
          new Error('MediatedLinkedRdfSourcesAsyncRdfIterator error'),
        );
        expect(await source.getSourceLinks({ baseURL: 'http://base.org/' })).toEqual([]);
      });
    });

    describe('isCloseable', () => {
      it('should be false for a non-empty link queue', async() => {
        const linkQueue = {
          isEmpty: () => false,
        };
        expect(source.isCloseable(linkQueue)).toEqual(false);
      });

      it('should be true for an empty link queue', async() => {
        const linkQueue = {
          isEmpty: () => true,
        };
        expect(source.isCloseable(linkQueue)).toEqual(true);
      });

      it('should be false for an empty link queue when sub-iterators are running', async() => {
        const linkQueue = {
          isEmpty: () => true,
        };
        source.iteratorsPendingCreation++;
        expect(source.isCloseable(linkQueue)).toEqual(false);
      });

      it('should be true for a non-empty link queue, but was forcefully closed', async() => {
        const linkQueue = {
          isEmpty: () => false,
        };
        source.iteratorsPendingCreation++;
        source.close();
        await new Promise(setImmediate);
        source.iteratorsPendingCreation--;
        expect(source.isCloseable(linkQueue)).toEqual(true);
      });

      it('should be false for non-empty link queue, was forcefully closed, and sub-iterators are running', async() => {
        const linkQueue = {
          isEmpty: () => true,
        };
        source.iteratorsPendingCreation++;
        source.close();
        await new Promise(setImmediate);
        expect(source.isCloseable(linkQueue)).toEqual(false);
      });
    });
  });
});
