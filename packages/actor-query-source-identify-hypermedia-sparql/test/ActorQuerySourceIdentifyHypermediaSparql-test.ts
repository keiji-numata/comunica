import { ActorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import { ActionContext, Bus } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { ActorQuerySourceIdentifyHypermediaSparql } from '../lib/ActorQuerySourceIdentifyHypermediaSparql';
import { QuerySourceSparql } from '../lib/QuerySourceSparql';

describe('ActorQuerySourceIdentifyHypermediaSparql', () => {
  let bus: any;
  let context: IActionContext;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    context = new ActionContext();
  });

  describe('The ActorQuerySourceIdentifyHypermediaSparql module', () => {
    it('should be a function', () => {
      expect(ActorQuerySourceIdentifyHypermediaSparql).toBeInstanceOf(Function);
    });

    it('should be a ActorQuerySourceIdentifyHypermediaSparql constructor', () => {
      expect(new (<any> ActorQuerySourceIdentifyHypermediaSparql)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorQuerySourceIdentifyHypermediaSparql);
      expect(new (<any> ActorQuerySourceIdentifyHypermediaSparql)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorQuerySourceIdentifyHypermedia);
    });

    it('should not be able to create new ActorQuerySourceIdentifyHypermediaSparql objects without \'new\'', () => {
      expect(() => { (<any> ActorQuerySourceIdentifyHypermediaSparql)(); }).toThrow();
    });
  });

  describe('An ActorQuerySourceIdentifyHypermediaSparql instance', () => {
    let actor: ActorQuerySourceIdentifyHypermediaSparql;

    beforeEach(() => {
      actor = new ActorQuerySourceIdentifyHypermediaSparql({
        name: 'actor',
        bus,
        mediatorHttp: <any> 'mediator',
        checkUrlSuffix: true,
        forceHttpGet: false,
        cacheSize: 1_024,
        bindMethod: 'values',
        countTimeout: 3_000,
      });
    });

    describe('#test', () => {
      it('should test with a forced sparql source type', async() => {
        expect(await actor.test({ url: 'URL', metadata: {}, quads: <any> null, forceSourceType: 'sparql', context }))
          .toEqual({ filterFactor: 1 });
      });

      it('should not test with a forced unknown source type', async() => {
        await expect(actor.test({ url: 'URL', metadata: {}, quads: <any> null, forceSourceType: 'unknown', context }))
          .rejects.toThrow(new Error('Actor actor is not able to handle source type unknown.'));
      });

      it('should test with a sparql service metadata', async() => {
        expect(await actor.test({ url: 'URL', metadata: { sparqlService: 'SERVICE' }, quads: <any> null, context }))
          .toEqual({ filterFactor: 1 });
      });

      it('should not test without a sparql service metadata', async() => {
        await expect(actor.test({ url: 'URL', metadata: {}, quads: <any> null, context })).rejects
          .toThrow(new Error('Actor actor could not detect a SPARQL service description or URL ending on /sparql.'));
      });

      it('should test with an URL ending with /sparql', async() => {
        expect(await actor.test({ url: 'URL/sparql', metadata: {}, quads: <any> null, context }))
          .toEqual({ filterFactor: 1 });
      });

      it('should not test with an URL ending with /sparql if checkUrlSuffix is false', async() => {
        actor = new ActorQuerySourceIdentifyHypermediaSparql({
          name: 'actor',
          bus,
          mediatorHttp: <any>'mediator',
          checkUrlSuffix: false,
          forceHttpGet: false,
          bindMethod: 'values',
          countTimeout: 3_000,
        });
        await expect(actor.test({ url: 'URL/sparql', metadata: {}, quads: <any> null, context })).rejects
          .toThrow(new Error('Actor actor could not detect a SPARQL service description or URL ending on /sparql.'));
      });

      it('should not test with an URL ending with /sparql if the type is forced to something else', async() => {
        actor = new ActorQuerySourceIdentifyHypermediaSparql({
          name: 'actor',
          bus,
          mediatorHttp: <any>'mediator',
          checkUrlSuffix: false,
          forceHttpGet: false,
          bindMethod: 'values',
          countTimeout: 3_000,
        });
        await expect(actor
          .test({ url: 'URL/sparql', metadata: {}, quads: <any> null, forceSourceType: 'file', context }))
          .rejects.toThrow(new Error('Actor actor is not able to handle source type file.'));
      });
    });

    describe('#run', () => {
      it('should return a source', async() => {
        const output = await actor
          .run({ url: 'URL', metadata: { sparqlService: 'SERVICE' }, quads: <any> null, context });
        expect(output.source).toBeInstanceOf(QuerySourceSparql);
        expect((<any> output.source).url).toEqual('SERVICE');
      });

      it('should return a source when no sparqlService was defined in metadata', async() => {
        const output = await actor
          .run({ url: 'URL', metadata: {}, quads: <any> null, forceSourceType: 'sparql', context });
        expect(output.source).toBeInstanceOf(QuerySourceSparql);
        expect((<any> output.source).url).toEqual('URL');
      });

      it('should return a source when no sparqlService was defined in metadata without forcing', async() => {
        const output = await actor
          .run({ url: 'URL', metadata: {}, quads: <any> null, context });
        expect(output.source).toBeInstanceOf(QuerySourceSparql);
        expect((<any> output.source).url).toEqual('URL');
      });

      it('should return a source with the correct cache size', async() => {
        const output = await actor
          .run({ url: 'URL', metadata: { sparqlService: 'SERVICE' }, quads: <any> null, context });
        expect((<any> output.source).cache.max).toEqual(1_024);
      });
    });
  });
});
